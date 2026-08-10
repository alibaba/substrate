# Substrate Performance 5-Round Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve Substrate + gVisor lifecycle performance over five measured rounds and produce a cumulative before/after performance report.

**Architecture:** Use the current perfkit summary as the baseline, then optimize one bottleneck class per round: worker scheduling collisions, worker capacity visibility, suspend worker release latency, atelet restore/checkpoint observability, and report trend capture. Each round must produce code, tests, regenerated reports, and a metrics delta.

**Tech Stack:** Go, ateapi control workflow, worker cache, perfkit JSONL/HTML reports, ACK + gVisor lifecycle tests.

---

### Task 1: Establish Baseline and Round Log

**Files:**
- Create: `results/performance-optimization-rounds.md`
- Read: `results/2026-07-14-ack-R0-smoke-ack-10n-substrate-gvisor-20260714T081402Z/reports/data/summary.json`

- [ ] **Step 1: Record baseline**

Write the baseline table with these values:

```text
r2-con3: 200 actors, concurrency 3, 1200/1200 success, 0.0% error, 6.04 ops/s
r2-con3 resume_boot p95: 1114 ms
r2-con3 resume_warm p95: 1072 ms
r2-con10: 200 actors, concurrency 10, 595/740 success, 19.6% error, 15.49 ops/s
r2-con10 resume_boot failures: 85/200
r2-con10 resume_warm failures: 60/115
Primary bottleneck: no free workers available
```

- [ ] **Step 2: Define per-round evidence fields**

Each round must record:

```text
Round:
Hypothesis:
Code change:
Validation command:
Cluster benchmark command:
Metrics before:
Metrics after:
Conclusion:
Follow-up:
```

### Task 2: Round 1 - Reduce Worker Pick Collisions

**Files:**
- Modify: `cmd/ateapi/internal/controlapi/workflow_resume.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_resume_test.go`

- [ ] **Step 1: Add a deterministic worker ranking test**

Add a test proving local-snapshot workers are preferred, then least-loaded deterministic order is used instead of random shuffle.

- [ ] **Step 2: Replace random shuffle with ranked selection**

Rank free workers by:

```text
1. node has local snapshot
2. lower worker pod name
3. lower namespace
```

This reduces collision jitter under concurrent resumes and makes behavior explainable.

- [ ] **Step 3: Validate**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi ./cmd/ateapi/internal/workercache
```

### Task 3: Round 2 - Improve Worker Capacity Visibility

**Files:**
- Modify: `cmd/ateapi/internal/workercache/workercache.go`
- Modify: `cmd/ateapi/internal/workercache/workercache_test.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_resume.go`

- [ ] **Step 1: Add worker cache stats**

Expose total/free/assigned worker counts from the cache without forcing every caller to scan raw workers.

- [ ] **Step 2: Include capacity stats in no-free-worker errors and logs**

When no worker is available, return/log enough context to distinguish:

```text
no eligible worker
eligible but assigned
local-snapshot restriction excluded all workers
cache not ready
```

- [ ] **Step 3: Validate**

Run:

```bash
go test ./cmd/ateapi/internal/workercache ./cmd/ateapi/internal/controlapi
```

### Task 4: Round 3 - Shorten Worker Release Critical Path

**Files:**
- Modify: `cmd/ateapi/internal/controlapi/workflow_suspend.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_suspend_test.go`

- [ ] **Step 1: Add tests for release-before-actor-clear ordering**

Verify a worker is released exactly once and actor fields are cleared after successful release.

- [ ] **Step 2: Remove avoidable extra reads**

Avoid a second `GetActor` in `FinalizeSuspendedStep` when the local state is already fresh enough after worker release.

- [ ] **Step 3: Validate**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi
```

### Task 5: Round 4 - Add Lifecycle Stage Evidence

**Files:**
- Modify: `cmd/ateapi/internal/controlapi/workflow.go`
- Modify: `benchmarking/perfkit/internal/perfdata/perfdata.go`

- [ ] **Step 1: Add duration logging for workflow steps**

Log duration and status for each workflow step so cluster runs can attribute latency to assignment, atelet call, or finalization.

- [ ] **Step 2: Add report section for optimization history**

Show per-round hypothesis and measured deltas in the HTML report.

- [ ] **Step 3: Validate**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi ./benchmarking/perfkit/...
```

### Task 6: Round 5 - Execute and Compare Cluster Rounds

**Files:**
- Modify: `results/performance-optimization-rounds.md`
- Generated: `results/<new-run-id>/raw/substrate-api/lifecycle-events-*.jsonl`
- Generated: `results/<new-run-id>/reports/index.html`

- [ ] **Step 1: Run stable baseline**

```bash
go run ./benchmarking/perfkit run-lifecycle \
  --kubeconfig <kubeconfig> \
  --run-id <run-id>-round5-con3 \
  --out results/<run-id>/raw/substrate-api/lifecycle-events-round5-con3.jsonl \
  --atespace perf-round5-con3 \
  --count 200 \
  --concurrency 3 \
  --node-scale 10 \
  --round round5-con3 \
  --stage optimized_con3 \
  --actor-prefix perf-r5c3
```

- [ ] **Step 2: Run capacity probe**

```bash
go run ./benchmarking/perfkit run-lifecycle \
  --kubeconfig <kubeconfig> \
  --run-id <run-id>-round5-con10 \
  --out results/<run-id>/raw/substrate-api/lifecycle-events-round5-con10.jsonl \
  --atespace perf-round5-con10 \
  --count 200 \
  --concurrency 10 \
  --node-scale 10 \
  --round round5-con10 \
  --stage optimized_con10 \
  --actor-prefix perf-r5c10
```

- [ ] **Step 3: Generate final comparison report**

```bash
go run ./benchmarking/perfkit report --run-dir results/<run-id> --run-id <run-id>
```

Compare:

```text
success rate
resume_boot p95
resume_warm p95
suspend_1 p95
suspend_2 p95
delete p95
ops/s
no-free-worker count
```
