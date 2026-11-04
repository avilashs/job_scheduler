# jobsched: a multi-tenant, Kubernetes-native DAG scheduler

Design and reference manifests for a distributed scheduler that runs DAGs of Apache Spark
(`SparkApplication`, via Spark Operator) and generic container (`batch/v1 Job`) tasks across many tenants.
Each tenant is confined by ResourceQuota, LimitRange, RBAC, NetworkPolicy, PriorityClass and
ValidatingAdmissionPolicy.

**Start here → [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** (design) · **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** (readiness + runbook)

> **Status:** the cluster guardrail layer (priorities, APF, admission policies, RBAC, tenant quotas and
> policies, Spark Operator values) is verified against a live v1.33 apiserver and ready for staging.
> The scheduler application itself is **not implemented** yet. See DEPLOYMENT.md §1 for the evidence and gaps.
> **Spark jobs can be scheduled on cron today** through the Spark Operator, inside all guardrails:
> see [DEPLOYMENT.md §4](docs/DEPLOYMENT.md#4-scheduling-spark-jobs).

| Section | Covers |
|---|---|
| §1 Architecture | Control plane (API, triggers, sharded scheduler, submitter, syncer, Postgres + outbox), execution plane, state machine, inter-task state passing |
| §2 Isolation | Namespace model, control-plane identities, quotas (overall + per-tier), RBAC, network policy (standard / strict per-attempt), admission policies, priority & preemption |
| §3 HA | Active-active over 256 fenced shards, idempotent triggers, deterministic K8s names |
| §4 Execution flow | Lifecycle of a Spark task, from footprint admission to the CRD, watch-driven state, retry, cancel and GC |
| §5–6 Scale | Sharding, cells, informers, APF, Spark driver client tuning, bottleneck table |
| §7 Observability | Metrics, dashboards, alerts |

## Layout

```
docs/ARCHITECTURE.md                 Design
docs/DEPLOYMENT.md                   Readiness assessment, prerequisites, step-by-step runbook, rollback
schema/metadata.sql                  Postgres DDL (state, fenced leases, admission ledger, outbox)
deploy/apiserver/                    kube-apiserver AdmissionConfiguration (self-managed control planes only)
deploy/cluster/                      PriorityClasses, APF, ValidatingAdmissionPolicies
deploy/control-plane/                Namespace and split-privilege RBAC
deploy/control-plane-workloads/      Reference scheduler Deployment + PDB (no image yet; not applied by default)
deploy/spark-operator/               Helm values for one Spark Operator shard
deploy/tenants/tenant-acme/          Complete per-tenant guardrail bundle (kustomize)
deploy/observability/                PrometheusRule (recording rules + alerts)
examples/                            DAG DSL, rendered SparkApplication, scheduled (cron) Spark job, per-attempt NetworkPolicy, SparkPi smoke test
test/policy-tests.sh                 59 guardrail checks; dry-run only, safe on live clusters
```

## Quick start (staging)

Follow [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md). It lists placeholders you must fill first (apiserver CIDR,
egress gateway, OIDC groups, quota sizes). The short version:

```bash
kubectl apply --server-side -f deploy/cluster/priorityclasses.yaml -f deploy/cluster/apf.yaml -f deploy/cluster/admission-policies.yaml
```

```bash
kubectl apply --server-side -f deploy/control-plane/
```

```bash
kubectl apply --server-side -k deploy/tenants/tenant-acme/
```

```bash
helm install spark-operator-0 spark-operator/spark-operator --version 2.2.1 -n spark-operator --create-namespace -f deploy/spark-operator/values-shard-0.yaml
```

```bash
test/policy-tests.sh tenant-acme
```
