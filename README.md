# jobsched: a multi-tenant, Kubernetes-native DAG scheduler

Design and reference manifests for a distributed scheduler that runs DAGs of Apache Spark
(`SparkApplication`, via Spark Operator) and generic container (`batch/v1 Job`) tasks across many tenants.
Each tenant is confined by ResourceQuota, LimitRange, RBAC, NetworkPolicy, PriorityClass and
ValidatingAdmissionPolicy.

**Start here → [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**

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
schema/metadata.sql              Postgres DDL (state, fenced leases, admission ledger, outbox)
deploy/cluster/                  PriorityClasses, APF, ValidatingAdmissionPolicies, apiserver admission config
deploy/control-plane/            Namespace, split-privilege RBAC, scheduler Deployment + PDB
deploy/tenants/tenant-acme/      Complete per-tenant guardrail bundle (kustomize)
deploy/observability/            PrometheusRule (recording rules + alerts)
examples/                        DAG DSL, rendered SparkApplication, per-attempt NetworkPolicy
```

## Applying the reference manifests

Requires Kubernetes ≥ 1.30 (ValidatingAdmissionPolicy v1, APF v1) and Spark Operator v2 CRDs installed.

```bash
kubectl apply -f deploy/cluster/priorityclasses.yaml -f deploy/cluster/apf.yaml -f deploy/cluster/admission-policies.yaml
kubectl apply -f deploy/control-plane/
kubectl apply -k deploy/tenants/tenant-acme/
```

`deploy/cluster/apiserver-admission-config.yaml` is a kube-apiserver flag file for self-managed
control planes. On managed Kubernetes, the `jobsched-tier-ceiling` policy enforces the same rule.
Before applying `networkpolicy.yaml`, replace the apiserver CIDR in `allow-driver-to-apiserver`.
