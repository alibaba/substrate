# Checkpoint Storage Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add checkpoint storage sub-stage telemetry, run memory-scale validation up to 16 GiB, and use the evidence to decide the next checkpoint optimization.

**Architecture:** Preserve the current `sparse-zstd-v1` format first. Extend atelet snapshot upload helpers to return per-file scan/compress/write/sync timing, aggregate those into checkpoint summaries, and feed the existing perfkit/raw result workflow. Defer a new chunked format until telemetry proves the single large-file path is the bottleneck.

**Tech Stack:** Go, gVisor/runsc CLI integration, Fluid/JindoFS snapshot filesystem, zstd, Kubernetes/ACK, perfkit JSONL/CSV/HTML.

---

### Task 1: Add Per-File Checkpoint Storage Stage Results

**Files:**
- Modify: `cmd/atelet/internal/ategcs/objects.go`
- Modify: `cmd/atelet/internal/ategcs/objects_test.go`

- [ ] **Step 1: Add a failing unit test for checkpoint upload stage timing**

Add a test that writes a sparse input file through the sparse-zstd upload helper and asserts the result contains non-zero logical bytes, populated bytes, compressed bytes, and non-negative stage durations for scan, compression, write, and total.

Run:

```bash
go test ./cmd/atelet/internal/ategcs -run 'Test.*Stage.*Result' -count=1
```

Expected: FAIL until the result struct exposes the required stage fields.

- [ ] **Step 2: Extend the result type**

Add fields to the existing snapshot I/O result struct:

```go
SparseScanDuration time.Duration
CompressionDuration time.Duration
StorageWriteDuration time.Duration
CloseDuration time.Duration
SyncDuration time.Duration
TotalDuration time.Duration
```

Use monotonic `time.Now()` measurements around the actual sparse scan, zstd encode, write, close, and sync/publish operations. Keep existing wrapper functions compatible.

- [ ] **Step 3: Verify the focused package**

Run:

```bash
go test ./cmd/atelet/internal/ategcs -count=1
```

Expected: PASS.

### Task 2: Emit Checkpoint File Stage Logs From Atelet

**Files:**
- Modify: `cmd/atelet/main.go`
- Modify: `cmd/atelet/main_test.go`

- [ ] **Step 1: Add a pure aggregation test**

Add a test for checkpoint file stage aggregation that verifies wall time is not confused with summed per-file time and that logical/populated/compressed byte totals are preserved.

Run:

```bash
go test ./cmd/atelet -run 'TestCheckpoint.*Stage' -count=1
```

Expected: FAIL until the aggregation helper exists.

- [ ] **Step 2: Add aggregation helper and structured logs**

Create a small helper in `cmd/atelet/main.go` that accepts per-file upload results and returns:

- file count
- total logical bytes
- total populated bytes
- total compressed bytes
- summed sparse scan time
- summed compression time
- summed storage write time
- summed close time
- summed sync time
- wall time

For each uploaded file, emit:

```text
Checkpoint file timing
```

with file name, bytes, format, backend, and the new stage durations. Extend:

```text
Checkpoint timing breakdown
```

with the aggregate fields.

- [ ] **Step 3: Verify atelet package**

Run:

```bash
go test ./cmd/atelet -count=1
```

Expected: PASS.

### Task 3: Preserve Raw Evidence in Perfkit

**Files:**
- Modify: `benchmarking/perfkit/README.md`
- Modify: `results/snapshot-critical-path-20260716T150500Z/process/snapshot-cache-progress-20260716T1900.md`

- [ ] **Step 1: Document log capture before rollout**

Update the perfkit README or process notes to state that atelet logs must be captured before changing the DaemonSet, because old pod logs disappear after rollout.

- [ ] **Step 2: Add the checkpoint matrix command template**

Document the diagnostic matrix for:

```text
64m, 256m, 512m, 1g, 2g, 4g, 8g, 16g
```

with safety gates between 2g, 4g, 8g, and 16g.

### Task 4: Local Verification

**Files:**
- Verify modified Go files and docs.

- [ ] **Step 1: Format Go files**

Run:

```bash
gofmt -w cmd/atelet/internal/ategcs/objects.go cmd/atelet/internal/ategcs/objects_test.go cmd/atelet/main.go cmd/atelet/main_test.go
```

- [ ] **Step 2: Run focused tests**

Run:

```bash
go test ./cmd/atelet/internal/ategcs ./cmd/atelet -count=1
```

Expected: PASS.

- [ ] **Step 3: Run current regression slice**

Run:

```bash
go test ./cmd/atelet/... ./cmd/ateapi/internal/controlapi ./cmd/ateapi/internal/store/ateredis ./demos/memory ./benchmarking/perfkit/... -count=1
```

Expected: PASS.

- [ ] **Step 4: Check diff hygiene**

Run:

```bash
git diff --check
```

Expected: PASS.

### Task 5: Build and Deploy Telemetry Image

**Files:**
- Generated: `/tmp/checkpoint-storage-build/`
- Generated: `results/snapshot-critical-path-20260716T150500Z/process/`

- [ ] **Step 1: Build atelet tarball**

Run:

```bash
mkdir -p /tmp/checkpoint-storage-build
ALL_PROXY=socks5h://127.0.0.1:5003 HTTPS_PROXY=socks5h://127.0.0.1:5003 HTTP_PROXY=socks5h://127.0.0.1:5003 \
KO_DOCKER_REPO=cache.invalid/substrate ko build ./cmd/atelet \
  --platform=linux/amd64 --bare --tags=checkpoint-storage-20260717 \
  --sbom=none --push=false \
  --tarball=/tmp/checkpoint-storage-build/atelet.tar
```

- [ ] **Step 2: Push via image-pusher**

Use the known reliable path:

1. Split the tarball into 2 MiB chunks.
2. Copy chunks into `ate-system/image-pusher`.
3. Reassemble and sha256-verify.
4. Push with `/ko-app/crane` to `192.168.53.189:5000/substrate/atelet:checkpoint-storage-20260717`.
5. Record immutable digest.

- [ ] **Step 3: Prepull and roll atelet**

Create a temporary prepull DaemonSet that runs:

```bash
chroot /host ctr -n k8s.io images pull --plain-http 192.168.53.189:5000/substrate/atelet@<digest>
```

Then update `ds/atelet`, preserve:

```text
ATE_SNAPSHOT_FORMAT=sparse-zstd-v1
ATE_SNAPSHOT_DIRECT_RESTORE=0
ATE_SNAPSHOT_DECODER_CONCURRENCY=4
ATE_SNAPSHOT_CACHE_MAX_BYTES=8589934592
```

Expected: `atelet` rolls to 10/10 Ready.

### Task 6: Run Checkpoint Telemetry Matrix

**Files:**
- Generated: `results/snapshot-critical-path-20260716T150500Z/raw/substrate-api/`
- Generated: `results/snapshot-critical-path-20260716T150500Z/derived/`
- Generated: `results/snapshot-critical-path-20260716T150500Z/process/`

- [ ] **Step 1: Run small diagnostic counts**

For each bucket:

```text
64m, 256m, 512m, 1g, 2g, 4g, 8g, 16g
```

run `run-lifecycle` with `--count 3 --concurrency 1`. Gate the larger sizes:

- stop before 4g if 2g fails cleanup;
- stop before 8g if 4g fails cleanup;
- stop before 16g if 8g fails cleanup.

- [ ] **Step 2: Capture logs before any rollout**

After each bucket or before any DaemonSet change, run:

```bash
KUBECONFIG=/tmp/ack-fluid.kubeconfig kubectl -n ate-system logs -l app=atelet --since=30m --tail=-1 > results/snapshot-critical-path-20260716T150500Z/raw/substrate-api/checkpoint-storage-<bucket>-atelet.log
```

- [ ] **Step 3: Generate summary**

Parse `Checkpoint file timing` and `Checkpoint timing breakdown` lines into a CSV with:

```text
bucket,file,logical_bytes,populated_bytes,compressed_bytes,sparse_scan_ms,compression_ms,storage_write_ms,close_ms,sync_ms,total_ms
```

- [ ] **Step 4: Decide next optimization**

If write/sync dominates, implement the smallest safe write/sync optimization in `sparse-zstd-v1`. If one large file dominates and write/sync cannot be safely reduced, write a new plan for `sparse-zstd-chunked-v2`.

### Task 7: Final Report

**Files:**
- Create or modify: `results/snapshot-critical-path-20260716T150500Z/reports/checkpoint-storage-decision.md`

- [ ] **Step 1: Write the decision report**

Include:

- image digest and deployed env;
- memory buckets attempted and skipped;
- success/error counts;
- checkpoint stage tables;
- whether checkpoint is dominated by scan, compression, write, sync, or manifest publish;
- explicit recommendation for v1 write/sync optimization or chunked v2;
- no P99 claim below 1,000 successful samples.

- [ ] **Step 2: Verify evidence references**

Confirm every numeric claim points to raw JSONL, stage logs, or generated CSV.
