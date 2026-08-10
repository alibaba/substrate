# Snapshot Critical-Path Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure and substantially reduce Substrate full-checkpoint and full-restore latency through Substrate-side storage-path changes without modifying gVisor.

**Architecture:** First add structured stage timing around atelet, ateom-gvisor, and snapshot I/O. Then add a feature-gated raw sparse Fluid format and direct immutable restore path, plus renewable workflow leases for large snapshots. Compare the current sparse-zstd path and candidates with deterministic memory profiles and repeated A/B rounds.

**Tech Stack:** Go, gRPC, runsc CLI integration, Fluid RWX filesystem, zstd, Kubernetes/ACK, perfkit JSONL/CSV/HTML.

---

### Task 1: Add Snapshot I/O Result Telemetry

**Files:**
- Modify: `cmd/atelet/internal/ategcs/objects.go`
- Modify: `cmd/atelet/internal/ategcs/objects_test.go`

- [ ] **Step 1: Write failing tests for byte and stage results**

Add tests that call the internal snapshot encode/decode helpers and assert a result containing logical, populated/stored/written bytes plus encode/decode duration. Include sparse and dense inputs.

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `go test ./cmd/atelet/internal/ategcs -run 'TestSnapshotIOResult' -count=1`

Expected: FAIL because the result-returning API does not exist.

- [ ] **Step 3: Implement result-returning APIs**

Introduce exported result structs and new `...WithResult` functions while preserving existing wrappers. Populate results from actual counters; do not parse log text.

- [ ] **Step 4: Run focused and package tests**

Run: `go test ./cmd/atelet/internal/ategcs -count=1`

Expected: PASS.

### Task 2: Add Atelet Checkpoint/Restore Critical-Path Summaries

**Files:**
- Modify: `cmd/atelet/main.go`
- Modify: `cmd/atelet/main_test.go`

- [ ] **Step 1: Write failing tests for stage aggregation**

Extract a pure stage accumulator and test that concurrent per-file results aggregate wall time separately from summed CPU/I/O time and retain logical/stored byte totals.

- [ ] **Step 2: Confirm RED**

Run: `go test ./cmd/atelet -run 'TestSnapshotStageSummary' -count=1`

Expected: FAIL because the accumulator does not exist.

- [ ] **Step 3: Instrument checkpoint and restore**

Emit one structured `Checkpoint timing breakdown` and one `Restore timing breakdown` record per operation. Record manifest, runtime, storage, materialization, OCI, and ateom durations, format/backend, byte totals, and failed stage.

- [ ] **Step 4: Verify**

Run: `go test ./cmd/atelet -count=1`

Expected: PASS.

### Task 3: Add Ateom-gVisor Runtime Stage Timings

**Files:**
- Modify: `cmd/ateom-gvisor/main.go`
- Create: `cmd/ateom-gvisor/main_test.go`

- [ ] **Step 1: Write failing tests for stage timer output**

Test a pure runtime-stage summary for checkpoint, network setup, pause restore, application restore, and readiness wait.

- [ ] **Step 2: Confirm RED**

Run: `go test ./cmd/ateom-gvisor -run 'TestRuntimeStageSummary' -count=1`

Expected: FAIL because no runtime-stage summary exists.

- [ ] **Step 3: Instrument existing calls without changing runsc behavior**

Wrap existing calls with monotonic timers and emit trace-correlated summaries. Do not add or change runsc flags.

- [ ] **Step 4: Verify**

Run: `go test ./cmd/ateom-gvisor -count=1`

Expected: PASS.

### Task 4: Add Versioned Raw Sparse Fluid Storage

**Files:**
- Modify: `cmd/atelet/internal/ategcs/objects.go`
- Modify: `cmd/atelet/internal/ategcs/objects_test.go`
- Modify: `cmd/atelet/sandbox_assets.go`
- Create: `cmd/atelet/sandbox_assets_test.go`

- [ ] **Step 1: Write failing round-trip and compatibility tests**

Cover `sparse-zstd-v1` and `raw-sparse-v1`, legacy manifests without a format, hole preservation, path traversal rejection, incomplete publication rejection, and checksum/size mismatch.

- [ ] **Step 2: Confirm RED**

Run: `go test ./cmd/atelet/... -run 'Test(RawSparse|SnapshotFormat|LegacySnapshot)' -count=1`

Expected: FAIL because versioned formats and raw helpers do not exist.

- [ ] **Step 3: Implement the raw format behind `ATE_SNAPSHOT_FORMAT`**

Keep `sparse-zstd-v1` as default. For `raw-sparse-v1`, copy sparse extents to a temporary Fluid directory, sync files, publish metadata, then publish the manifest last. Readers select behavior from the manifest and remain compatible with legacy compressed snapshots.

- [ ] **Step 4: Verify**

Run: `go test ./cmd/atelet/... -count=1`

Expected: PASS.

### Task 5: Add Safe Direct Restore Eligibility

**Files:**
- Modify: `cmd/atelet/main.go`
- Modify: `cmd/atelet/main_test.go`
- Modify: `cmd/atelet/internal/ategcs/objects.go`

- [ ] **Step 1: Write failing eligibility tests**

Test that direct restore is allowed only for immutable, fully published `raw-sparse-v1` snapshots under the configured snapshot filesystem and is rejected for object fallback, legacy manifests, unsafe paths, or missing files.

- [ ] **Step 2: Confirm RED**

Run: `go test ./cmd/atelet -run 'TestDirectRestore' -count=1`

Expected: FAIL because eligibility selection does not exist.

- [ ] **Step 3: Implement `ATE_SNAPSHOT_DIRECT_RESTORE`**

Pass the validated immutable Fluid snapshot directory to ateom instead of materializing a second local copy. Preserve the current local materialization path as fallback. Do not change runsc itself.

- [ ] **Step 4: Add mutation detection integration check**

Hash and stat representative snapshot files before and after a restore in the cluster harness. Reject direct mode if runsc changes source files.

- [ ] **Step 5: Verify**

Run: `go test ./cmd/atelet/... -count=1`

Expected: PASS.

### Task 6: Replace Fixed Workflow Timeout with Renewable Lease

**Files:**
- Modify: `cmd/ateapi/internal/controlapi/workflow.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_resume_test.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_suspend_test.go`
- Modify: `cmd/ateapi/internal/store/store.go`
- Modify: `cmd/ateapi/internal/store/ateredis/ateredis.go`
- Modify: `cmd/ateapi/internal/store/ateredis/ateredis_test.go`

- [ ] **Step 1: Write failing lease tests**

Use a short fake TTL to prove renewal preserves ownership during a long operation, loss of ownership cancels the workflow, and completion releases the lease exactly once.

- [ ] **Step 2: Confirm RED**

Run: `go test ./cmd/ateapi/internal/controlapi ./internal/... -run 'Test.*Lease' -count=1`

Expected: FAIL because lock renewal is not implemented.

- [ ] **Step 3: Implement compare-and-renew**

Renew only when key and ownership token still match. Separate operation safety deadline from lease TTL. Keep default behavior compatible for short operations and expose explicit configuration for benchmark safety deadlines.

- [ ] **Step 4: Verify race safety**

Run: `go test -race ./cmd/ateapi/internal/controlapi ./internal/... -count=1`

Expected: PASS.

### Task 7: Add Representative Memory Profiles

**Files:**
- Modify: `demos/memory/main.go`
- Create: `demos/memory/main_test.go`
- Modify: `demos/memory/memory.yaml.tmpl`

- [ ] **Step 1: Write failing deterministic profile tests**

Test `zeroed`, `sparse-randomish`, and `dense-random` with a seed. Assert deterministic checksums, full-buffer dense writes, and differing compressibility.

- [ ] **Step 2: Confirm RED**

Run: `go test ./demos/memory -count=1`

Expected: FAIL because dense seeded generation is absent.

- [ ] **Step 3: Implement profiles and status metadata**

Fill every byte for `dense-random` using a deterministic low-overhead generator. Expose profile, seed, requested bytes, resident length, and checksum on the status endpoint.

- [ ] **Step 4: Verify**

Run: `go test ./demos/memory -count=1`

Expected: PASS.

### Task 8: Extend Perfkit for Stage Evidence and A/B Runs

**Files:**
- Modify: `benchmarking/perfkit/internal/perfdata/perfdata.go`
- Modify: `benchmarking/perfkit/internal/perfdata/perfdata_test.go`
- Modify: `benchmarking/perfkit/internal/runner/runner.go`
- Modify: `benchmarking/perfkit/main.go`
- Modify: `benchmarking/perfkit/main_test.go`

- [ ] **Step 1: Write failing parser/report tests**

Add fixtures for structured checkpoint/restore stage records and assert per-round stage CSV output, byte throughput, format/backend dimensions, and failed-stage counts.

- [ ] **Step 2: Confirm RED**

Run: `go test ./benchmarking/perfkit/... -run 'Test.*Stage' -count=1`

Expected: FAIL because stage evidence is not represented.

- [ ] **Step 3: Implement stage ingestion and reporting**

Produce `snapshot-stage-summary.csv`, retain raw stage JSONL, and render per-round key-path comparisons. Label P99 exploratory below 1,000 successes.

- [ ] **Step 4: Add run metadata and residue checks**

Capture DaemonSet image/env, snapshot format, profile/seed, node readiness, and pre/post actor residues for every round.

- [ ] **Step 5: Verify**

Run: `go test ./benchmarking/perfkit/... -count=1`

Expected: PASS.

### Task 9: Build, Deploy, and Run Diagnostic A/B Matrix

**Files:**
- Generated: `results/snapshot-critical-path-20260716T150500Z/`

- [ ] **Step 1: Run local verification**

Run: `gofmt -w cmd/atelet/internal/ategcs/objects.go cmd/atelet/internal/ategcs/objects_test.go cmd/atelet/main.go cmd/atelet/main_test.go cmd/atelet/sandbox_assets.go cmd/atelet/sandbox_assets_test.go cmd/ateom-gvisor/main.go cmd/ateom-gvisor/main_test.go cmd/ateapi/internal/controlapi/workflow.go cmd/ateapi/internal/controlapi/workflow_resume_test.go cmd/ateapi/internal/controlapi/workflow_suspend_test.go cmd/ateapi/internal/store/store.go cmd/ateapi/internal/store/ateredis/ateredis.go cmd/ateapi/internal/store/ateredis/ateredis_test.go demos/memory/main.go demos/memory/main_test.go benchmarking/perfkit/internal/perfdata/perfdata.go benchmarking/perfkit/internal/perfdata/perfdata_test.go benchmarking/perfkit/internal/runner/runner.go benchmarking/perfkit/main.go benchmarking/perfkit/main_test.go && go test ./cmd/atelet/... ./cmd/ateom-gvisor ./cmd/ateapi/internal/controlapi ./demos/memory ./benchmarking/perfkit/... -count=1`

Expected: PASS.

- [ ] **Step 2: Build and push immutable atelet, ateom, and memory images**

Record image digests in the run directory. Use the existing in-cluster registry and localhost replacement path.

- [ ] **Step 3: Execute diagnostic matrix**

For 64, 256, and 512 MiB, execute at least three independent rounds per candidate/profile. Interleave compressed and raw variants in A-B-B-A order. Use unique namespaces and snapshot prefixes.

- [ ] **Step 4: Analyze stage evidence**

Reject variants that move time into Fluid I/O/runsc, increase errors, or amplify storage without repeatable end-to-end benefit. Select direct raw, raw materialized, compressed, or decoded-cache follow-up from evidence.

### Task 10: Run Large-Memory and Confirmation Tests

**Files:**
- Generated: `results/snapshot-critical-path-20260716T150500Z/`
- Create: `results/snapshot-critical-path-20260716T150500Z/reports/decision-report.md`

- [ ] **Step 1: Test lease behavior with 1 GiB and 2 GiB**

Confirm operations can exceed 28 seconds without losing lock ownership or leaving stranded actors. Enter 4 GiB only after 2 GiB cleanup is clean.

- [ ] **Step 2: Run confirmation rounds**

Run at least 100 successful samples per selected memory bucket for P95. Do not claim stable P99 below 1,000 successful restores.

- [ ] **Step 3: Restore cluster configuration and verify residues**

Restore the captured pre-test DaemonSet configuration unless the selected image is explicitly retained for continued testing. Confirm all run-specific actors, namespaces, and temporary resources are removed.

### Task 11: Add a Node-Local Decoded Snapshot Cache Candidate

**Files:**
- Modify: `internal/ateompath/ateompath.go`
- Create: `cmd/atelet/snapshot_cache.go`
- Create: `cmd/atelet/snapshot_cache_test.go`
- Modify: `cmd/atelet/main.go`
- Modify: `cmd/atelet/main_test.go`
- Modify: `pkg/proto/ateapipb/ateapi.proto`
- Modify: `internal/proto/ateletpb/atelet.proto`
- Modify: `cmd/ateapi/internal/controlapi/workflow_suspend.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_resume.go`
- Modify: corresponding generated proto and workflow tests

- [ ] **Step 1: Write failing cache publication and lookup tests**

Cover URI-key isolation, path traversal rejection, atomic publication, incomplete-entry misses, immutable existing entries, and bounded oldest-entry eviction. A cache miss must leave the external sparse-zstd restore path usable.

- [ ] **Step 2: Confirm RED**

Run: `go test ./cmd/atelet -run 'TestSnapshotCache' -count=1`

Expected: FAIL because the node-local decoded cache does not exist.

- [ ] **Step 3: Publish without another large-file copy**

After the external compressed snapshot and manifest are durable, move the checkpoint files into a temporary cache entry on the same node filesystem, write the cache manifest last, fsync, and atomically rename the entry. Keep the feature gated and retain the external compressed snapshot as the portable source of truth.

- [ ] **Step 4: Add safe direct cache restore with fallback**

On a cache hit, replace the per-actor `restore-state` directory with a symlink to the immutable cache entry. On miss or rejected/incomplete cache state, use the existing download/decompress path. Never point runsc at a cache entry that can be evicted while a restore or restored actor may still read it.

- [ ] **Step 5: Add soft locality preference**

Return cache-publication status from atelet checkpoint, store the originating node as a hint on the external snapshot record, and prefer a free eligible worker on hinted nodes. If none is free, select any free eligible worker so the external snapshot remains a valid fallback. Golden snapshots without hints remain portable.

- [ ] **Step 6: Verify focused and regression tests**

Run: `go test ./cmd/atelet/... ./cmd/ateapi/internal/controlapi ./cmd/ateapi/internal/store/ateredis -count=1`

Expected: PASS. Also compile-test ateom-gvisor for Linux.

- [ ] **Step 7: Run interleaved cache-off/cache-on A/B rounds**

Use the same 64, 256, and 512 MiB workload profiles and raw JSONL reporting. Report hit rate, download/decode time, ateom restore time, end-to-end latency, errors, and fallback count. Reject the cache if locality reduces usable capacity or if gains do not repeat across rounds.

- [ ] **Step 4: Generate final report**

Report each round, aggregated P50/P95, failure rate, critical-path stage changes, bytes/second, CPU/storage cost, rejected hypotheses, and remaining boundary. Every claim links to raw evidence.

### Task 11: Final Verification

**Files:**
- Verify all modified files and generated reports.

- [ ] **Step 1: Run focused test suite and formatting checks**

Run: `go test ./cmd/atelet/... ./cmd/ateom-gvisor ./cmd/ateapi/internal/controlapi ./demos/memory ./benchmarking/perfkit/... -count=1 && git diff --check`

Expected: PASS.

- [ ] **Step 2: Run repository verification appropriate to changed modules**

Run: `make verify`

Expected: PASS, or record pre-existing unrelated failures separately with evidence.

- [ ] **Step 3: Audit requirements and evidence**

Confirm the final report contains no unsupported percentage, no hidden failed samples, and no P99 claim below the documented sample threshold.
