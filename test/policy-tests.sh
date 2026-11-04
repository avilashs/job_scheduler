#!/usr/bin/env bash
# Post-deployment guardrail verification. Safe to run against a real cluster: every write
# uses --dry-run=server, so admission (PSA, LimitRange, ResourceQuota, ValidatingAdmissionPolicy)
# runs in full but nothing is persisted. RBAC checks use `kubectl auth can-i` with impersonation.
#
# Requirements: kubectl >= 1.30, cluster-admin (for --as impersonation), the manifests in deploy/
# already applied, tenant namespace $NS onboarded with max-tier=critical (bundle incl. scheduling-interim.yaml),
# and a Spark Operator shard installed whose controller SA is $OPERATOR_SA.
#
# Usage: test/policy-tests.sh [tenant-namespace]      (default: tenant-acme)
set -uo pipefail
NS=${1:-tenant-acme}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
SUB=system:serviceaccount:jobsched-system:jobsched-submitter
WATCH=system:serviceaccount:jobsched-system:jobsched-watcher
TOP=system:serviceaccount:jobsched-system:jobsched-tenant-operator
DRV=system:serviceaccount:$NS:spark-driver
# Spark Operator controller SA. The chart names it <release>-controller; override with OPERATOR_SA.
OPSA=${OPERATOR_SA:-system:serviceaccount:spark-operator:spark-operator-0-controller}
DEVG="oidc:$NS-developers"
EXE=system:serviceaccount:$NS:spark-executor
pass=0; fail=0
ok()  { echo "  PASS  $1"; pass=$((pass+1)); }
bad() { echo "  FAIL  $1"; [ -n "${2:-}" ] && echo "        $2"; fail=$((fail+1)); }

# expect allow|deny "<description>" <file-or-> [kubectl args...]   (manifest on stdin when file is -)
expect() {
  local want=$1 desc=$2 src=$3; shift 3
  local out; out=$(kubectl create --dry-run=server -f "$src" "$@" 2>&1); local rc=$?
  if echo "$out" | grep -q "failed calling webhook"; then bad "$desc (a webhook is unreachable; the result says nothing about policy)" "$(echo "$out" | grep -o 'failed calling webhook "[^"]*"' | head -1)"
  elif [ "$want" = allow ] && [ $rc -eq 0 ]; then ok "$desc"
  elif [ "$want" = deny ] && [ $rc -ne 0 ]; then
    # A denial only counts if admission (VAP, RBAC, quota, LimitRange, PSA) produced it. A schema
    # error or a typo in the test object would otherwise pass as a "correct" denial.
    local why; why=$(echo "$out" | grep -oE "(denied request|is forbidden|cannot (create|update)|exceeded quota|violates PodSecurity|insufficient quota|must be less than or equal|maximum (cpu|memory)|max limit to request ratio)[^\"]{0,90}" | head -1)
    if [ -n "$why" ]; then ok "$desc  -> $why"; else bad "$desc (denied, but not by admission)" "$(echo "$out" | head -2)"; fi
  else bad "$desc (wanted $want, rc=$rc)" "$(echo "$out" | head -2)"; fi
}
can() { # can yes|no "<desc>" <user> <verb> <resource> [ns] [extra args]
  local want=$1 desc=$2 user=$3 verb=$4 res=$5 ns=${6:-}; shift 6 2>/dev/null || shift $#
  local got; got=$(kubectl auth can-i "$verb" "$res" ${ns:+-n "$ns"} --as "$user" "$@" 2>/dev/null)
  [ "$got" = "$want" ] && ok "$desc" || bad "$desc (wanted $want, got $got)"
}

# Build a SparkApplication from the example with a field changed (yq-free: python + PyYAML).
spark_variant() { python3 - "$ROOT/examples/sparkapplication.yaml" "$NS" "$1" <<'PY'
import sys, yaml, json
d = yaml.safe_load(open(sys.argv[1])); d['metadata']['namespace'] = sys.argv[2]
exec(sys.argv[3]); print(json.dumps(d))
PY
}
# ScheduledSparkApplication from the example with a mutation applied to d.
sched_variant() { python3 - "$ROOT/examples/scheduled-sparkapplication.yaml" "$NS" "$1" <<'PY'
import sys, yaml, json
d = yaml.safe_load(open(sys.argv[1])); d['metadata']['namespace'] = sys.argv[2]
exec(sys.argv[3]); print(json.dumps(d))
PY
}
# The SparkApplication the operator creates at a cron tick (mirrors spark-operator v2.2.1
# scheduledsparkapplication controller: name <name>-<UnixNano>, labels copied + scheduled-app-name,
# spec = template verbatim).
scheduled_run() { python3 - "$ROOT/examples/scheduled-sparkapplication.yaml" "$NS" "${1:-pass}" <<'PY'
import sys, yaml, json, time
d = yaml.safe_load(open(sys.argv[1])); name = d['metadata']['name']
labels = dict(d['metadata'].get('labels', {})); labels['sparkoperator.k8s.io/scheduled-app-name'] = name
r = {"apiVersion": "sparkoperator.k8s.io/v1beta2", "kind": "SparkApplication",
     "metadata": {"name": f"{name}-{time.time_ns()}", "namespace": sys.argv[2], "labels": labels},
     "spec": d['spec']['template']}
exec(sys.argv[3]); print(json.dumps(r))
PY
}
# Native Spark mode: render with the real tool, select one object by kind, optionally mutate it (o).
native() { # native <Job|CronJob|ConfigMap|DriverPod> <python mutation of o> [tool args...]
  local kind=$1 mut=$2; shift 2
  "$ROOT/tools/jobsched-spark-submit" --name np-test --namespace "$NS" --class org.apache.spark.examples.SparkPi \
      "$@" /opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar 10 | python3 -c '
import sys, yaml, json
kind, mut = sys.argv[1], sys.argv[2]
docs = list(yaml.safe_load_all(sys.stdin))
if kind == "DriverPod":   # the pod the Job controller creates from the Job template
    job = next(d for d in docs if d["kind"] == "Job")
    t = job["spec"]["template"]
    o = {"apiVersion": "v1", "kind": "Pod",
         "metadata": {"generateName": "np-test-", "namespace": job["metadata"]["namespace"], "labels": t["metadata"]["labels"]},
         "spec": t["spec"]}
else:
    o = next(d for d in docs if d["kind"] == kind)
exec(mut); print(json.dumps(o))' "$kind" "$mut"
}
# Executor pod exactly as real Spark 3.5.3 built it (test/fixtures), optionally mutated (o).
spark_executor() { python3 - "$ROOT/test/fixtures/spark-3.5.3-executor-pod.json" "$NS" "${1:-pass}" <<'PY'
import sys, json
o = json.load(open(sys.argv[1])); o.pop('_comment', None); o['metadata']['namespace'] = sys.argv[2]
exec(sys.argv[3]); print(json.dumps(o))
PY
}
pod() { # pod <priorityClass> [python mutation of p]
python3 - "$NS" "$1" "${2:-pass}" <<'PY'
import sys, json
ns, pc, mut = sys.argv[1:4]
p = {"apiVersion": "v1", "kind": "Pod", "metadata": {"generateName": "policy-test-", "namespace": ns},
     "spec": {"containers": [{"name": "c", "image": "registry.k8s.io/pause:3.10",
       "resources": {"requests": {"cpu": "1", "memory": "1Gi"}, "limits": {"cpu": "1", "memory": "1Gi"}},
       "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]},
                           "runAsNonRoot": True, "runAsUser": 65535, "seccompProfile": {"type": "RuntimeDefault"}}}]}}
if pc: p["spec"]["priorityClassName"] = pc
exec(mut); print(json.dumps(p))
PY
}

echo "== SparkApplication guardrails (VAP jobsched-sparkapplication-guardrails)"
expect allow "rendered example is admitted (as submitter)" <(spark_variant "pass") --as $SUB
expect deny "driver with a non-spark-driver ServiceAccount" <(spark_variant "d['spec']['driver']['serviceAccount']='default'") --as $SUB
expect deny "operator-managed retries (restartPolicy OnFailure)" <(spark_variant "d['spec']['restartPolicy']={'type':'OnFailure'}") --as $SUB
expect deny "missing timeToLiveSeconds" <(spark_variant "del d['spec']['timeToLiveSeconds']") --as $SUB
expect deny "driver/executor attempt-id mismatch" <(spark_variant "d['spec']['executor']['labels']['jobsched.io/attempt-id']='999'") --as $SUB
expect deny "hostPath volume" <(spark_variant "d['spec']['volumes']=[{'name':'h','hostPath':{'path':'/'}}]") --as $SUB

echo "== Pod tier ceiling, hygiene, PSA, LimitRange (VAP jobsched-tier-ceiling)"
expect allow "critical pod in a max-tier=critical namespace" <(pod jobsched-critical-driver)
expect deny "pod without a priorityClassName" <(pod "")
expect deny "pod using a non-jobsched PriorityClass" <(pod system-cluster-critical)
expect deny "literal secret-like env var" <(pod jobsched-adhoc-executor "p['spec']['containers'][0]['env']=[{'name':'DB_PASSWORD','value':'hunter2'}]")
expect allow "secret-like env var via secretKeyRef" <(pod jobsched-adhoc-executor "p['spec']['containers'][0]['env']=[{'name':'DB_PASSWORD','valueFrom':{'secretKeyRef':{'name':'s','key':'k'}}}]")
expect deny "hostNetwork" <(pod jobsched-adhoc-executor "p['spec']['hostNetwork']=True")
expect deny "PSA restricted: no securityContext" <(pod jobsched-adhoc-executor "p['spec']['containers'][0]['securityContext']={}")
expect allow "container with no resources gets LimitRange defaults" <(pod jobsched-adhoc-executor "del p['spec']['containers'][0]['resources']")
gpus=$(kubectl create --dry-run=server -o jsonpath='{.spec.containers[0].resources.requests}' -f <(pod jobsched-adhoc-executor "del p['spec']['containers'][0]['resources']") 2>&1)
case "$gpus" in *gpu*) bad "defaulted container must not request GPUs" "$gpus";; *) ok "defaulted container requests no GPUs ($gpus)";; esac
expect deny "container above LimitRange max cpu" <(pod jobsched-adhoc-executor "p['spec']['containers'][0]['resources']={'requests':{'cpu':'32','memory':'1Gi'},'limits':{'cpu':'32','memory':'1Gi'}}")
echo "== Scheduled Spark jobs (interim cron: ScheduledSparkApplication)"
expect allow "example nightly schedule (as tenant developer)" <(sched_variant "pass") --as dev --as-group $DEVG
expect allow "every-5-minutes schedule" <(sched_variant "d['spec']['schedule']='*/5 * * * *'") --as dev --as-group $DEVG
expect allow "@daily schedule" <(sched_variant "d['spec']['schedule']='@daily'") --as dev --as-group $DEVG
expect deny "every-minute schedule" <(sched_variant "d['spec']['schedule']='* * * * *'") --as dev --as-group $DEVG
expect deny "every-2-minutes schedule" <(sched_variant "d['spec']['schedule']='*/2 * * * *'") --as dev --as-group $DEVG
expect deny "@every 30s schedule" <(sched_variant "d['spec']['schedule']='@every 30s'") --as dev --as-group $DEVG
expect deny "concurrencyPolicy Allow" <(sched_variant "d['spec']['concurrencyPolicy']='Allow'") --as dev --as-group $DEVG
expect deny "missing concurrencyPolicy" <(sched_variant "del d['spec']['concurrencyPolicy']") --as dev --as-group $DEVG
expect deny "run history above 5" <(sched_variant "d['spec']['successfulRunHistoryLimit']=50") --as dev --as-group $DEVG
expect deny "name too long for run names" <(sched_variant "d['metadata']['name']='a-very-long-scheduled-spark-application'") --as dev --as-group $DEVG
expect deny "template with a non-spark-driver ServiceAccount" <(sched_variant "d['spec']['template']['driver']['serviceAccount']='default'") --as dev --as-group $DEVG
expect deny "template with 10 operator retries" <(sched_variant "d['spec']['template']['restartPolicy']['onFailureRetries']=10") --as dev --as-group $DEVG
expect deny "template missing timeToLiveSeconds" <(sched_variant "del d['spec']['template']['timeToLiveSeconds']") --as dev --as-group $DEVG
expect deny "template with a hostPath volume" <(sched_variant "d['spec']['template']['volumes']=[{'name':'h','hostPath':{'path':'/'}}]") --as dev --as-group $DEVG
expect allow "cron tick: run created by the operator from the template" <(scheduled_run) --as $OPSA
expect deny "OnFailure retries outside a schedule (as submitter)" <(scheduled_run) --as $SUB
expect deny "OnFailure retries without the scheduled-app label (as operator)" <(scheduled_run "del r['metadata']['labels']['sparkoperator.k8s.io/scheduled-app-name']") --as $OPSA
can yes "tenant developers can manage their schedules"           dev create scheduledsparkapplications.sparkoperator.k8s.io $NS --as-group "$DEVG"
can yes "tenant developers can cancel a run"                     dev delete sparkapplications.sparkoperator.k8s.io $NS --as-group "$DEVG"
can no  "tenant developers cannot schedule in another namespace" dev create scheduledsparkapplications.sparkoperator.k8s.io default --as-group "$DEVG"


echo "== Spark native mode (driver = Job pod in client mode; tools/jobsched-spark-submit)"
expect allow "rendered executor-template ConfigMap (as submitter)" <(native ConfigMap pass) --as $SUB
expect allow "rendered Spark driver Job (as submitter)" <(native Job pass) --as $SUB
expect allow "driver pod the Job controller creates (as spark-driver's namespace)" <(native DriverPod pass)
expect allow "rendered Spark CronJob (as tenant developer)" <(native CronJob pass --schedule '30 3 * * *' --retries 2) --as dev --as-group $DEVG
expect deny "Job without ttlSecondsAfterFinished" <(native Job "del o['spec']['ttlSecondsAfterFinished']") --as $SUB
expect deny "Job with implicit backoffLimit (defaults to 6)" <(native Job "del o['spec']['backoffLimit']") --as $SUB
expect deny "Job without activeDeadlineSeconds" <(native Job "del o['spec']['activeDeadlineSeconds']") --as $SUB
expect deny "Job running as the default ServiceAccount" <(native Job "o['spec']['template']['spec']['serviceAccountName']='default'") --as $SUB
expect deny "every-minute CronJob" <(native CronJob pass --schedule '* * * * *') --as dev --as-group $DEVG
expect deny "CronJob with concurrencyPolicy Allow" <(native CronJob "o['spec']['concurrencyPolicy']='Allow'" --schedule '30 3 * * *') --as dev --as-group $DEVG
expect deny "CronJob whose jobTemplate uses the default ServiceAccount" <(native CronJob "o['spec']['jobTemplate']['spec']['template']['spec']['serviceAccountName']='default'" --schedule '30 3 * * *') --as dev --as-group $DEVG
expect deny "CronJob with 10 retries" <(native CronJob "o['spec']['jobTemplate']['spec']['backoffLimit']=10" --schedule '30 3 * * *') --as dev --as-group $DEVG
expect allow "real Spark 3.5.3 executor pod, created by spark-driver" <(spark_executor) --as $DRV
expect deny "real Spark executor without the template's PriorityClass" <(spark_executor "del o['spec']['priorityClassName']") --as $DRV
expect deny "real Spark executor without the template's securityContext" <(spark_executor "o['spec']['containers'][0].pop('securityContext')") --as $DRV
expect deny "real Spark executor created with the executor's own token" <(spark_executor) --as $EXE
can no  "tenant developers cannot create Jobs directly"          dev create jobs.batch $NS --as-group "$DEVG"
can yes "tenant developers can manage CronJobs"                  dev create cronjobs.batch $NS --as-group "$DEVG"
\
echo "== Submitter NetworkPolicy shape (VAP jobsched-submitter-networkpolicy-shape)"
expect allow "per-attempt policy (as submitter)" <(sed "s/namespace: tenant-acme/namespace: $NS/" "$ROOT/examples/per-attempt-networkpolicy.yaml") --as $SUB
expect deny "broad allow-all policy (as submitter)" - --as $SUB <<NP
{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"evil","namespace":"$NS"},
 "spec":{"podSelector":{},"ingress":[{}],"egress":[{}]}}
NP
expect deny "per-attempt policy with an ipBlock peer (as submitter)" - --as $SUB <<NP
{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"evil2","namespace":"$NS"},
 "spec":{"podSelector":{"matchLabels":{"jobsched.io/attempt-id":"1"}},"egress":[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0"}}]}]}}
NP

echo "== Tenant operator scope (VAP jobsched-tenant-operator-scope)"
expect deny "operator binds cluster-admin inside a tenant namespace" - --as $TOP <<RB
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"x","namespace":"$NS"},
 "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"cluster-admin"},
 "subjects":[{"kind":"User","name":"mallory","apiGroup":"rbac.authorization.k8s.io"}]}
RB
expect deny "operator writes a RoleBinding in kube-system" - --as $TOP <<RB
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"x","namespace":"kube-system"},
 "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"jobsched-tenant-viewer"},
 "subjects":[{"kind":"User","name":"mallory","apiGroup":"rbac.authorization.k8s.io"}]}
RB
expect allow "operator binds an approved role in a tenant namespace" - --as $TOP <<RB
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"x","namespace":"$NS"},
 "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"jobsched-tenant-viewer"},
 "subjects":[{"kind":"Group","name":"oidc:new-team","apiGroup":"rbac.authorization.k8s.io"}]}
RB
expect deny "operator creates a namespace outside the tenant- prefix" - --as $TOP <<<'{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"rogue","labels":{"jobsched.io/tenant":"x"}}}'
can no  "operator cannot create pods in kube-system"            $TOP create pods kube-system
out=$(kubectl label ns kube-system jobsched.io/tenant=x --dry-run=server --as $TOP 2>&1) \
  && bad "operator relabels kube-system as a tenant (wanted deny)" "$out" || ok "operator cannot relabel kube-system as a tenant"
expect allow "operator creates a tenant-* namespace" - --as $TOP <<<'{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"tenant-newco","labels":{"jobsched.io/tenant":"newco"}}}'


echo "== RBAC"
can yes "submitter creates SparkApplications in tenant ns"     $SUB create sparkapplications.sparkoperator.k8s.io $NS
can no  "submitter cannot write outside onboarded namespaces"  $SUB create sparkapplications.sparkoperator.k8s.io default
can no  "submitter cannot read Secrets"                        $SUB get secrets $NS
can no  "submitter cannot edit ResourceQuotas"                 $SUB update resourcequotas $NS
can yes "watcher lists pods cluster-wide"                      $WATCH list pods "" --all-namespaces
can no  "watcher cannot create anything"                       $WATCH create pods $NS
can no  "watcher cannot read Secrets"                          $WATCH list secrets "" --all-namespaces
can yes "spark-driver creates executor pods in its namespace"  $DRV create pods $NS
can no  "spark-driver cannot create pods in other namespaces"  $DRV create pods default
can no  "spark-driver cannot read Secrets via the API"         $DRV get secrets $NS
can no  "spark-executor has no API access"                     $EXE list pods $NS
can yes "tenant developers can read pods"                      dev get pods $NS --as-group "oidc:$NS-developers"
can no  "tenant developers cannot bypass the scheduler"        dev create sparkapplications.sparkoperator.k8s.io $NS --as-group "oidc:$NS-developers"

echo
echo "Result: $pass passed, $fail failed"
[ $fail -eq 0 ]
