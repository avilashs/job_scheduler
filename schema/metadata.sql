-- Metadata store (PostgreSQL 16). Every row carries tenant_id; the API also enables
-- row-level security so a bug in tenant filtering cannot leak another tenant's rows.

CREATE TYPE task_state AS ENUM (
  'PENDING','QUEUED','ADMITTED','SUBMITTED','RUNNING',
  'SUCCESS','FAILED','RETRYING','PREEMPTED','LOST',
  'UPSTREAM_FAILED','SKIPPED','CANCELLED');

CREATE TYPE tier AS ENUM ('critical','standard','adhoc');

CREATE TABLE tenant (
  tenant_id     text PRIMARY KEY,
  namespace     text NOT NULL UNIQUE,
  cell          text NOT NULL,
  max_tier      tier NOT NULL DEFAULT 'standard',
  share_weight  int  NOT NULL DEFAULT 1,          -- weighted DRF for fair share
  network_mode  text NOT NULL DEFAULT 'standard'
);

-- Admission ledger: hard_* mirrors ResourceQuota.hard (refreshed from the informer);
-- reserved_* is the sum of footprints of admitted, not-yet-terminal attempts.
-- tier = NULL is the overall ceiling; a row per tier mirrors the scoped quotas.
CREATE TABLE tenant_budget (
  tenant_id          text REFERENCES tenant,
  tier               tier,                          -- NULL => overall
  hard_cpu_m         bigint NOT NULL,
  hard_mem_b         bigint NOT NULL,
  hard_gpu           int    NOT NULL DEFAULT 0,
  max_concurrent_apps int   NOT NULL,
  overcommit         numeric(4,2) NOT NULL DEFAULT 1.0,
  reserved_cpu_m     bigint NOT NULL DEFAULT 0,
  reserved_mem_b     bigint NOT NULL DEFAULT 0,
  reserved_gpu       int    NOT NULL DEFAULT 0,
  running_apps       int    NOT NULL DEFAULT 0,
  CHECK (reserved_gpu <= hard_gpu),
  UNIQUE NULLS NOT DISTINCT (tenant_id, tier)    -- PG15+: at most one overall (NULL-tier) row per tenant
);

CREATE TABLE dag (
  dag_id      bigserial PRIMARY KEY,
  tenant_id   text NOT NULL REFERENCES tenant,
  name        text NOT NULL,
  shard_id    smallint NOT NULL,                  -- hash(tenant_id, name) % 256
  paused      boolean NOT NULL DEFAULT false,
  current_version int,
  UNIQUE (tenant_id, name)
);

CREATE TABLE dag_version (
  dag_id      bigint REFERENCES dag,
  version     int,
  spec        jsonb NOT NULL,                     -- immutable once published
  spec_sha256 bytea NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  created_by  text NOT NULL,
  PRIMARY KEY (dag_id, version)
);

CREATE TABLE dag_run (
  dag_run_id   bigserial PRIMARY KEY,
  dag_id       bigint NOT NULL REFERENCES dag,
  tenant_id    text   NOT NULL,
  version      int    NOT NULL,
  logical_ts   timestamptz NOT NULL,
  trigger_kind text   NOT NULL,                   -- cron | kafka | webhook | manual
  trigger_key  text   NOT NULL,                   -- idempotency key (logical_ts, event id, Idempotency-Key ...)
  state        text   NOT NULL DEFAULT 'RUNNING',
  params       jsonb  NOT NULL DEFAULT '{}',
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (dag_id, trigger_kind, trigger_key)      -- split-brain layer 2: duplicate triggers collide here
);

CREATE TABLE task_instance (
  task_instance_id bigserial PRIMARY KEY,
  dag_run_id   bigint NOT NULL REFERENCES dag_run,
  tenant_id    text   NOT NULL,
  task_name    text   NOT NULL,
  tier         tier   NOT NULL,
  state        task_state NOT NULL DEFAULT 'PENDING',
  attempts     int    NOT NULL DEFAULT 0,
  max_attempts int    NOT NULL,
  next_eligible_at timestamptz,                   -- retry backoff
  UNIQUE (dag_run_id, task_name)
);
CREATE INDEX task_instance_active ON task_instance (tenant_id, tier, next_eligible_at)
  WHERE state IN ('QUEUED','RETRYING');

CREATE TABLE task_attempt (
  attempt_id     bigserial,
  task_instance_id bigint NOT NULL,
  tenant_id      text   NOT NULL,
  shard_id       smallint NOT NULL,
  shard_epoch    bigint NOT NULL,                 -- split-brain layer 1: fenced writes
  attempt_no     int    NOT NULL,
  k8s_kind       text   NOT NULL,                 -- SparkApplication | Job
  k8s_namespace  text   NOT NULL,
  k8s_name       text   NOT NULL,                 -- deterministic (layer 3)
  state          task_state NOT NULL,
  reason         text,
  footprint      jsonb  NOT NULL,                 -- {cpu_m, mem_b, gpu}
  last_rv        bigint NOT NULL DEFAULT 0,       -- highest resourceVersion applied
  cancel_requested boolean NOT NULL DEFAULT false,
  created_at     timestamptz NOT NULL DEFAULT now(),
  submitted_at   timestamptz,
  started_at     timestamptz,
  finished_at    timestamptz,
  PRIMARY KEY (attempt_id, created_at),
  UNIQUE (task_instance_id, attempt_no, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX task_attempt_inflight ON task_attempt (shard_id, state)
  WHERE state IN ('ADMITTED','SUBMITTED','RUNNING');
CREATE INDEX task_attempt_k8s ON task_attempt (k8s_namespace, k8s_name);

CREATE TABLE task_output (
  task_instance_id bigint NOT NULL,
  key        text NOT NULL,
  value      jsonb NOT NULL,
  CHECK (pg_column_size(value) <= 65536),
  PRIMARY KEY (task_instance_id, key)
);

-- Append-only audit of every transition (partitioned, retained 400 days).
CREATE TABLE state_transition (
  id          bigserial,
  attempt_id  bigint,
  task_instance_id bigint NOT NULL,
  from_state  task_state,
  to_state    task_state NOT NULL,
  reason      text,
  actor       text NOT NULL,                      -- replica id / user
  at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, at)
) PARTITION BY RANGE (at);

-- Transactional outbox: written in the same txn as the transition, relayed to Kafka.
CREATE TABLE outbox (
  id         bigserial,
  tenant_id  text NOT NULL,
  topic      text NOT NULL,
  key        text NOT NULL,
  payload    jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Fenced shard leases (docs/ARCHITECTURE.md §3).
CREATE TABLE shard_lease (
  shard_id   smallint PRIMARY KEY CHECK (shard_id BETWEEN 0 AND 255),
  owner      text,
  epoch      bigint NOT NULL DEFAULT 0,
  expires_at timestamptz NOT NULL DEFAULT 'epoch'
);
INSERT INTO shard_lease (shard_id) SELECT g FROM generate_series(0,255) g;

-- Example of a fenced transition used by the syncer (layer 1 plus monotonic resourceVersion):
--   UPDATE task_attempt a
--      SET state = 'RUNNING', started_at = now(), last_rv = $rv, shard_epoch = $epoch
--    WHERE a.attempt_id = $id AND a.last_rv < $rv AND a.state = 'SUBMITTED'
--      AND EXISTS (SELECT 1 FROM shard_lease l
--                   WHERE l.shard_id = a.shard_id AND l.owner = $me
--                     AND l.epoch = $epoch AND l.expires_at > now());
