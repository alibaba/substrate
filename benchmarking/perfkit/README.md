# Substrate Perfkit

`benchmarking/perfkit` is a repeatable performance test helper for Substrate
ACK + gVisor validation. It keeps raw JSONL events, derived CSV files, a
machine-readable summary, an HTML report, and a manifest with SHA-256 hashes.

## Generate A Report From An Existing Run

```bash
go run ./benchmarking/perfkit report \
  --run-dir results/<run-id> \
  --run-id <run-id>
```

Default input files are discovered from:

```text
raw/substrate-api/lifecycle-events*.jsonl
```

Generated outputs:

```text
derived/all-lifecycle-events.jsonl
derived/round-summary.csv
derived/latency-percentiles.csv
derived/error-summary.csv
reports/data/summary.json
reports/data/charts.json
reports/index.html
manifest.json
```

## Preflight A Cluster

Run this before using a new ACK cluster as a perfkit validation environment:

```bash
go run ./benchmarking/perfkit preflight \
  --kubeconfig /path/to/kubeconfig \
  --out results/<run-id>/raw/preflight/new-env.json \
  --require-substrate=true
```

The command exits non-zero when the Kubernetes API is unreachable, no Ready
real ECS nodes are present, `ate-system` is missing, or core Substrate pods are
not Ready. Use `--require-substrate=false` only for an infrastructure-only
probe before Substrate has been deployed.

A new-environment tool validation is not complete until this command passes
with `--require-substrate=true` on that environment, followed by at least one
`run-lifecycle` round and a generated report in a fresh run directory.

## Prepare ACK Nodes For gVisor

ACK images may ship with `user.max_user_namespaces=0`. That disables Linux user
namespaces and makes gVisor `runsc create` fail with errors like:

```text
cannot create gofer process: fork/exec /proc/self/exe: no space left on device
```

Prepare every ACK node before gVisor lifecycle tests:

```bash
go run ./benchmarking/perfkit prepare-ack-nodes \
  --kubeconfig /path/to/kubeconfig \
  --out results/<run-id>/process/prepare-ack-nodes.jsonl \
  --debug-image <ACK-or-ACR-reachable-busybox-image> \
  --user-max-user-namespaces 28633
```

The command creates a short-lived privileged pod per node, sets and verifies
`user.max_user_namespaces`, writes one JSONL record per node, then deletes the
temporary pod. Use a VPC-reachable image mirror on ACK clusters where public
image pulls are blocked.

## Run Lifecycle Workload

```bash
go run ./benchmarking/perfkit run-lifecycle \
  --kubeconfig /path/to/kubeconfig \
  --run-id <run-id>-r2con3 \
  --out results/<run-id>/raw/substrate-api/lifecycle-events-r2con3.jsonl \
  --atespace perf-r2-con3 \
  --count 200 \
  --concurrency 3 \
  --node-scale 10 \
  --round r2-con3 \
  --stage ateapi_persistent_client_concurrent \
  --actor-prefix perf-r2c3
```

The command writes one JSONL event per lifecycle operation:

```text
create -> resume_boot -> suspend_1 -> resume_warm -> suspend_2 -> delete
```

If any actor sequence fails, the command exits non-zero and leaves the raw
events intact. Use `cleanup-atespace` before the next round.

Before changing `atelet` DaemonSet images or environment between performance
rounds, capture `atelet` logs for the round. Kubernetes deletes old DaemonSet
pods during rollout, so old pod logs may be unavailable after the configuration
change:

```bash
KUBECONFIG=/path/to/kubeconfig kubectl -n ate-system logs -l app=atelet \
  --since=30m --tail=-1 > results/<run-id>/raw/substrate-api/<round>-atelet.log
```

For checkpoint storage diagnostics, run memory buckets in this order and stop
at the first unsafe large-memory boundary:

```text
64m -> 256m -> 512m -> 1g -> 2g -> 4g -> 8g -> 16g
```

Only continue from 2g to 4g, 4g to 8g, or 8g to 16g after the previous bucket
has no stranded actors, crashed workers, lock ownership errors, or cleanup
failures.

## Cleanup Residual Actors

```bash
go run ./benchmarking/perfkit cleanup-atespace \
  --kubeconfig /path/to/kubeconfig \
  --atespace perf-r2-con10 \
  --out results/<run-id>/process/cleanup-perf-r2-con10.jsonl \
  --concurrency 10
```

Cleanup lists actors in the target atespace, attempts `SuspendActor`, then
`DeleteActor`, verifies the atespace is empty, and deletes the atespace.

## ACK Scale Workflow

Perfkit records `--node-scale` as a test label. It does not change ACK nodepool
size directly. Scale ACK nodepools as an explicit outer orchestration step, log
the change in `raw/cluster-scaling-events.jsonl`, wait for nodes and `atelet`
pods to become ready, then run the lifecycle workload.

For the current ACK environment, the observed worker pool was smaller than the
10-node count. Treat `FailedPrecondition: no free workers available` during
`ResumeActor` as a capacity signal and retain it in the report rather than
discarding the round.
