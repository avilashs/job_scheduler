# Deployment Guide

> Status as of this revision: **the guardrail layer is ready for a staging rollout. The scheduler itself is not deployable yet: no implementation exists.**
> Read §1 before deploying anything.

---

## 1. Readiness assessment

### 1.1 What this repository is

This repository contains the **design** ([ARCHITECTURE.md](ARCHITECTURE.md)) and the **cluster-side configuration**
that enforces tenant isolation. It does **not** contain the scheduler application: no API, trigger service,
scheduler, submitter, syncer or Tenant Operator code, and no container image. Everything below is marked
by how far it has actually been verified.

| Component | Files | Status | Evidence |
|---|---|---|---|
| PriorityClasses | `deploy/cluster/priorityclasses.yaml` | ✅ Ready | Applied to a live v1.33 apiserver. Quota scoping and preemption policy were exercised. |
| API Priority & Fairness | `deploy/cluster/apf.yaml` | ✅ Ready | Applied. Live requests from the watcher and spark-driver identities were classified into `jobsched-control-plane` and `spark-drivers`. No dangling FlowSchemas. |
| Admission policies (5 VAPs) | `deploy/cluster/admission-policies.yaml` | ✅ Ready | Applied. All CEL type-checks are clean. 39 allow/deny cases pass (`test/policy-tests.sh`). |
| apiserver quota lock | `deploy/apiserver/admission-config.yaml` | ✅ Ready (self-managed only) | kube-apiserver started with it. A critical pod in a namespace without critical quota was refused. |
| Control-plane RBAC | `deploy/control-plane/` | ✅ Ready | `can-i` matrix in the test suite. No identity can read Secrets. The tenant operator is confined. |
| Tenant bundle | `deploy/tenants/tenant-acme/` | ✅ Ready, after you fill the placeholders (§2.2) | Applied. Quota exhaustion verified with real pods (7×16 CPU admitted, the 8th refused at 120). LimitRange defaults verified. |
| Spark Operator values | `deploy/spark-operator/values-shard-0.yaml` | ✅ Ready | Chart v2.2.1 rendered with these values and applied in a server-side dry run. Webhook scoping and RBAC confirmed in the rendered output. |
| Prometheus rules | `deploy/observability/prometheusrule.yaml` | ⚠️ Partial | Applied against the PrometheusRule CRD. The `kube_resourcequota` and apiserver metric names were checked against a live v1.33 apiserver. **Not** checked with `promtool`. All `jobsched_*` metrics depend on code that does not exist. |
| Postgres schema | `schema/metadata.sql` | ⚠️ Partial | Loads on PostgreSQL 16. Lease takeover and ledger admission were exercised. Row-level security and partition automation are **not** included (step 5). |
| NetworkPolicies | `deploy/tenants/tenant-acme/networkpolicy.yaml` | ⚠️ Schema-valid only | **Enforcement not tested**: the verification environment had no CNI. Run the connectivity checks in step 9. |
| Scheduler / API / submitter / syncer / triggers | — | ❌ **Not implemented** | Only `deploy/control-plane-workloads/scheduler.yaml` exists, as a reference shape. Its image does not exist. |
| Tenant Operator | — | ❌ **Not implemented** | Onboarding is manual (step 6) until it exists. |

### 1.2 Defects found and fixed in this review

| # | Severity | Defect | Fix |
|---|---|---|---|
| 1 | **Critical (security)** | The tenant-operator ClusterRole could create pods cluster-wide, including in `kube-system`, and could label any namespace. Labelling `kube-system` as a tenant would also have bypassed the policy that confines its RBAC writes. | Spark driver permissions moved to ClusterRole `jobsched-spark-driver`. The operator now has only `bind` on it. A new admission rule limits the operator to `tenant-*` namespaces. |
| 2 | **Critical (functional)** | The LimitRange's GPU `max` without a `default` made Kubernetes default **every container to 4 GPUs**. CPU-only pods would never schedule, and GPU quota was consumed. | Removed the GPU `max`. GPU caps come from ResourceQuota only. |
| 3 | **High (functional)** | The default memory limit/request (2Gi/1Gi = 2.0) violated `maxLimitRequestRatio: 1.5`, so every pod that omitted resources was rejected. | The default limit is now 1536Mi. |
| 4 | **High (design)** | The architecture recommended avoiding the Spark Operator's mutating webhook. That webhook is what applies `priorityClassName`, security contexts, env and secret volumes. Without it, every Spark pod is rejected. | The doc and the values file now require the webhook, with `failurePolicy: Fail`. |
| 5 | High (alerting) | `JobschedShardsUnowned` could not fire when *all* scheduler replicas were down, because the metric is absent. | Added `or vector(0)`, plus a `JobschedSchedulerAbsent` alert. |
| 6 | Medium | A tenant RoleBinding referenced an operator ClusterRole and ServiceAccount that the Helm chart does not create under those names. | Removed it. The chart creates the operator's RBAC in each `jobNamespaces` entry, as verified in the rendered chart. |
| 7 | Medium | The API needs TokenReview for output callbacks but had no identity for it. | Added SA `jobsched-api` bound to `system:auth-delegator`. |
| 8 | Medium | The tenant-operator VAP's `roleRef` expression failed CEL type-checking for non-RoleBinding kinds. It worked only through short-circuiting. | Split into two policies. All five now type-check cleanly. |
| 9 | Low | Prometheus could not reach the Spark metrics servlet (port 4040). | Port 4040 added to `allow-observability`. |
| 10 | Low | The scheduler used `system-cluster-critical`. | New `jobsched-control-plane` PriorityClass. |
| 11 | Low (operability) | `kubectl apply -f deploy/cluster/` failed on the apiserver flag file, and `-f deploy/control-plane/` created crash-looping pods. | Moved both out of those directories. |

### 1.3 Verdict

* **Safe to deploy to a staging cluster now:** steps 1–7 and 9. This puts the tenant guardrails, quotas,
  priorities, APF and Spark Operator in place. Workloads can run through the guardrails, for example the SparkPi
  smoke test submitted as the submitter identity.
* **Before production:** verify NetworkPolicy enforcement on your CNI (step 9.3), run promtool over the rules,
  roll the VAPs out in `Warn` first (step 2), and size the quotas to your actual capacity (§2.2).
* **Not deployable:** the scheduler control plane (step 8), until it is implemented.

---

## 2. Prerequisites

### 2.1 Platform

| Requirement | Why | Check |
|---|---|---|
| Kubernetes **≥ 1.30** (verified on 1.33.1) | `ValidatingAdmissionPolicy` v1, APF v1 | `kubectl version` |
| A CNI that **enforces** NetworkPolicy (Calico, Cilium, Antrea; on EKS the VPC CNI with network policy enabled) | Tenant network isolation. Policies are silently ignored without it. | step 9.3 |
| Prometheus Operator + kube-state-metrics | `PrometheusRule`, `kube_resourcequota` | `kubectl get crd prometheusrules.monitoring.coreos.com` |
| PostgreSQL 16 HA (CloudNativePG recommended) | Metadata store (`UNIQUE NULLS NOT DISTINCT` needs PG ≥ 15) | — |
| Helm ≥ 3.14 | Spark Operator | `helm version` |
| An egress gateway (or Cilium FQDN policies) | `allow-egress-data-plane` | — |
| External Secrets Operator (recommended) | Tenant secrets from Vault | — |
| `cluster-admin` for installation | Cluster-scoped objects, impersonation in tests | `kubectl auth can-i '*' '*'` |
| `python3` with PyYAML on the operator's workstation | `test/policy-tests.sh` | `python3 -c 'import yaml'` |

### 2.2 Placeholders to fill before applying

| File | Placeholder | Replace with |
|---|---|---|
| `deploy/tenants/tenant-acme/networkpolicy.yaml` | `ipBlock: 10.0.0.0/28` in `allow-driver-to-apiserver` | Your apiserver endpoint IPs: `kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes -o jsonpath='{.items[*].endpoints[*].addresses[*]}'`. Use `/32` per address. |
| same file | `egress-gateway` namespace / `egress.jobsched.io/tenant: acme` | Your egress gateway's namespace and pod labels, or replace the policy with your CNI's FQDN policy |
| `deploy/tenants/tenant-acme/resourcequota.yaml` | All sizes | Your capacity contract. **Invariant:** the sum of `tier-critical` across tenants ≤ allocatable capacity − system reserve. |
| `deploy/tenants/tenant-acme/rbac.yaml` | `oidc:tenant-acme-developers` | The group claim your OIDC provider issues (check `--oidc-groups-prefix`) |
| `deploy/tenants/tenant-acme/namespace.yaml` | `jobsched.io/max-tier`, `network-mode`, `operator-shard` | The tenant's entitlement |
| `deploy/cluster/apf.yaml` | `spark-drivers` subjects | One entry per tenant namespace |
| `deploy/spark-operator/values-shard-0.yaml` | `spark.jobNamespaces` | The tenant namespaces this shard owns |

---

## 3. Deployment steps

Run from the repository root. Each step ends with a check; do not continue until it passes.

### Step 1: apiserver admission config (self-managed control planes only)

Skip this step on EKS, GKE or AKS. There, the `jobsched-tier-ceiling` VAP enforces the same rule.

Copy `deploy/apiserver/admission-config.yaml` to each control-plane node (for example
`/etc/kubernetes/admission/admission.yaml`). Add `--admission-control-config-file=<path>` to kube-apiserver and
mount the directory into its static pod. Restart one apiserver at a time.

> If you already pass an `AdmissionConfiguration`, **merge** the `ResourceQuota` plugin entry into it. Do not replace it.

**Check** (after step 2, so that the PriorityClass exists):

```bash
kubectl create ns quota-lock-check
```

```bash
kubectl run t -n quota-lock-check --image=registry.k8s.io/pause:3.10 --dry-run=server \
  --overrides='{"spec":{"priorityClassName":"jobsched-critical-driver"}}'
```

Expected result: `insufficient quota to match these scopes`. Clean up afterwards:

```bash
kubectl delete ns quota-lock-check
```

### Step 2: Cluster-scoped objects

```bash
kubectl apply --server-side -f deploy/cluster/priorityclasses.yaml -f deploy/cluster/apf.yaml -f deploy/cluster/admission-policies.yaml
```

**Existing cluster? Roll the policies out in Warn mode first.** They only act on namespaces labelled
`jobsched.io/tenant`, plus the three platform identities, so other workloads are unaffected. Still, start safely:

```bash
for b in $(kubectl get validatingadmissionpolicybindings -o name | grep jobsched); do kubectl patch $b --type merge -p '{"spec":{"validationActions":["Warn","Audit"]}}'; done
```

Switch back to `["Deny","Audit"]` after step 9 passes.

**Check:** every policy type-checks cleanly. The `WARNINGS` column must be `<none>`:

```bash
kubectl get validatingadmissionpolicy -o custom-columns=NAME:.metadata.name,WARNINGS:.status.typeChecking.expressionWarnings
```

```bash
kubectl get flowschema jobsched-control-plane spark-operator spark-drivers
```

### Step 3: Control-plane namespace and identities

```bash
kubectl apply --server-side -f deploy/control-plane/
```

**Check:**

```bash
kubectl auth can-i list secrets -A --as system:serviceaccount:jobsched-system:jobsched-watcher
```

Expected result: `no`.

### Step 4: Spark Operator (one Helm release per shard)

Install the CRDs with the chart, into namespace **`spark-operator`** (the APF FlowSchema matches that namespace).
Adjust `spark.jobNamespaces` per shard first.

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
```

```bash
helm install spark-operator-0 spark-operator/spark-operator --version 2.2.1 -n spark-operator --create-namespace -f deploy/spark-operator/values-shard-0.yaml
```

The tenant namespaces listed in `jobNamespaces` must exist before this install, so on a fresh cluster run step 6
first and come back here. When you onboard a tenant later, add it to a shard's list and run `helm upgrade` for
that shard.

**Check:** the controller and webhook are Ready, and the webhook is limited to tenant namespaces:

```bash
kubectl -n spark-operator get deploy
```

```bash
kubectl get mutatingwebhookconfiguration -o yaml | grep -A4 namespaceSelector
```

### Step 5: Metadata database

1. Provision PostgreSQL 16 with a synchronous standby (CloudNativePG: `instances: 3`, `minSyncReplicas: 1`) and
   PgBouncer in transaction mode.
2. Load the schema:

   ```bash
   psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -f schema/metadata.sql
   ```

3. **Create partitions.** Inserts into `task_attempt`, `state_transition` and `outbox` fail without one. Use
   pg_partman, or at minimum create the current and next month on a schedule:

   ```sql
   CREATE TABLE task_attempt_2026_11     PARTITION OF task_attempt     FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
   CREATE TABLE state_transition_2026_11 PARTITION OF state_transition FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
   CREATE TABLE outbox_2026_11           PARTITION OF outbox           FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
   ```

4. **Row-level security** for the API role (not in the DDL). Example for one table, repeated for every table with
   `tenant_id`. The scheduler and syncer use a separate role with `BYPASSRLS`.

   ```sql
   ALTER TABLE dag_run ENABLE ROW LEVEL SECURITY;
   CREATE POLICY tenant_isolation ON dag_run USING (tenant_id = current_setting('jobsched.tenant_id'));
   ```

5. Create the connection Secret. Replace the placeholder values, or better, sync it with External Secrets:

   ```bash
   kubectl -n jobsched-system create secret generic jobsched-db --from-literal=dsn='postgres://jobsched:REDACTED@pgbouncer:5432/jobsched?sslmode=verify-full'
   ```

### Step 6: Onboard a tenant

Until the Tenant Operator exists, onboard by hand (or with GitOps) from a copy of the reference bundle:

```bash
cp -r deploy/tenants/tenant-acme deploy/tenants/tenant-<name>
```

Then, in the copy:

* Replace every `tenant-acme` / `acme` with the new name and size the quotas.
* Fill the placeholders listed in §2.2.

```bash
kubectl apply --server-side -k deploy/tenants/tenant-<name>/
```

After applying:

* Add `{namespace: tenant-<name>, name: spark-driver}` to the `spark-drivers` FlowSchema subjects in
  `deploy/cluster/apf.yaml` and re-apply it.
* Add the namespace to a Spark Operator shard's `spark.jobNamespaces` and run `helm upgrade` (step 4).
* Insert the matching `tenant` and `tenant_budget` rows (overall plus one per tier, with sizes equal to the
  ResourceQuotas).

**Check:**

```bash
kubectl -n tenant-<name> get resourcequota,limitrange,networkpolicy,rolebinding
```

### Step 7: Observability

```bash
kubectl apply --server-side -f deploy/observability/prometheusrule.yaml
```

The `release: kube-prometheus-stack` label must match your Prometheus `ruleSelector`. Then:

* Add a PodMonitor for `spark-role=driver` pods on port 4040, path `/metrics/executors/prometheus`. Drop the
  `spark_app_id` label so cardinality stays bounded.
* Import the four dashboards described in ARCHITECTURE.md §7.3.

**Check:** `jobsched:tenant_quota_utilization:ratio` returns series in Prometheus. That recording rule needs only
kube-state-metrics, so it works before the scheduler exists. Before production, run the rules through promtool:

```bash
promtool check rules <(kubectl get prometheusrule -n monitoring jobsched -o jsonpath='{.spec}' | python3 -c 'import sys,json,yaml;print(yaml.safe_dump(json.load(sys.stdin)))')
```

### Step 8: Scheduler control plane (**blocked: not implemented**)

When the implementation and image exist:

```bash
kubectl apply --server-side -f deploy/control-plane-workloads/
```

Deploy in this order: API, then trigger service, then scheduler and syncer, then submitter. Each must reach Ready.
The scheduler is Ready only once its informers have synced and it holds at least one shard. Each mode runs as its
own Deployment under its own ServiceAccount: the scheduler and syncer as `jobsched-watcher`, the submitter as
`jobsched-submitter`, the API as `jobsched-api`.

### Step 9: Verify

#### 9.1 Guardrail suite (safe on live clusters: dry-run only)

```bash
test/policy-tests.sh tenant-acme
```

Expected result: `Result: 39 passed, 0 failed`. Run it for every tenant namespace you onboard.

#### 9.2 End-to-end Spark smoke test (exercises the webhook, PSA, quotas and network policy)

```bash
kubectl create -f examples/smoke-sparkpi.yaml --as system:serviceaccount:jobsched-system:jobsched-submitter
```

```bash
kubectl -n tenant-acme get sparkapplication js-smoke-sparkpi-a1 -w
```

Expected result: `COMPLETED`, and the driver log contains `Pi is roughly 3.14`. If it hangs in `SUBMITTED` or
the executors never register, check:

* driver pod events (quota or VAP denials);
* `kubectl -n tenant-acme get pod -l spark-role=driver -o jsonpath='{..priorityClassName}'` (empty means the
  webhook did not mutate the pod);
* NetworkPolicy, if the driver cannot reach the apiserver: the CIDR placeholder from §2.2.

#### 9.3 NetworkPolicy enforcement (not yet verified anywhere; do this before production)

Start a test pod in the tenant namespace. The overrides give it the tier, labels and security context the policies
require:

```bash
kubectl -n tenant-acme run np-probe --image=busybox:1.36 --restart=Never --labels=jobsched.io/managed=true,spark-role=executor \
  --overrides='{"spec":{"priorityClassName":"jobsched-adhoc-executor","securityContext":{"runAsNonRoot":true,"runAsUser":65534,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"np-probe","image":"busybox:1.36","command":["sleep","600"],"securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}}]}}'
```

Run these probes from it:

* **must fail:** egress to the internet: `kubectl -n tenant-acme exec np-probe -- wget -T3 -qO- http://example.com`
* **must fail:** another tenant's pod IP, and `kube-system` pods other than DNS
* **must succeed:** `nslookup kubernetes.default`
* **must fail from executors:** the apiserver IP on 443 (only drivers are allowed)

Clean up afterwards:

```bash
kubectl -n tenant-acme delete pod np-probe
```

#### 9.4 Preemption

On staging, fill a node pool with `adhoc` pods, then submit a `critical` pod. Confirm that the adhoc pods are
evicted, and that an adhoc pod never evicts anything (`preemptionPolicy: Never`).

---

## 4. Rollback

| Undo | Command | Notes |
|---|---|---|
| Admission policies (fastest de-risk) | Patch the bindings to `["Warn","Audit"]` (step 2) | Keeps visibility, stops denials. |
| Tenant | `kubectl delete -k deploy/tenants/tenant-<name>/` | Deletes NetworkPolicies, so traffic opens up. Prefer deleting the namespace if the tenant is leaving. |
| Spark Operator shard | `helm uninstall spark-operator-0 -n spark-operator` | Running apps continue, but CRs are no longer reconciled. |
| Cluster objects | Delete in reverse order: policies, then APF, then PriorityClasses | PriorityClasses cannot be deleted while pods use them. Delete the pods first. |
| apiserver flag | Remove `--admission-control-config-file` and restart the apiservers one at a time | |

---

## 5. How this was verified

* A real **kube-apiserver v1.33.1 + etcd 3.5 + kube-controller-manager**. kind could not start pods in the review
  sandbox (nested runc), so no kubelet or CNI was available. Everything at the API and admission layer is therefore
  real, but nothing that needs running pods is (Spark execution, NetworkPolicy enforcement, preemption).
* The Spark Operator CRD and Helm chart **v2.2.1** (official release), and the PrometheusRule CRD (v0.83.0).
* **PostgreSQL 16**, loaded with the schema plus scripted lease and ledger scenarios.
* Not run: `promtool` (the review environment did not allow downloading and executing it). Use the command in step 7.
