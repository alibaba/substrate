# Snapshot Critical-Path Optimization Design

## Objective

Deliver a large, evidence-backed reduction in Substrate full-checkpoint and
full-restore latency without changing gVisor. The work will optimize only the
Substrate control plane, atelet data path, storage layout, and benchmark
instrumentation. No percentage improvement is assumed before stage-level
measurements establish the available headroom.

## Evidence and Problem Statement

The July 16 memory-gradient run shows that full restore scales with actor
memory while cold boot does not:

| Memory | Cold resume average | Warm resume average | Suspend average |
|---:|---:|---:|---:|
| 64 MiB | 398 ms | 979 ms | 3.13 s |
| 256 MiB | 624 ms | 4.42 s | 4.83 s |
| 512 MiB | 943 ms | 9.64 s | 7.48 s |
| 1 GiB | 1.55 s | 17.46 s | 13.11 s |

The incremental warm-restore throughput is approximately 49--66 MiB/s. The
incremental suspend throughput is approximately 91--113 MiB/s. In contrast,
cold resume scales at approximately 800 MiB/s. This isolates the large cost to
the full snapshot path, not actor creation or OCI preparation.

The current external-snapshot path already:

- stores snapshots through a Fluid RWX filesystem;
- processes snapshot files concurrently;
- overlaps checkpoint download with OCI preparation;
- uses sparse-extent encoding and zstd `SpeedFastest`;
- publishes a manifest last;
- uses an unpacked OCI rootfs cache.

However, a Fluid-backed restore still reads compressed files, decodes them,
reconstructs local sparse files, and then asks runsc to read those files. A
checkpoint writes local runsc images, scans and compresses them, writes them to
Fluid, syncs them, and publishes the manifest. The previous memory run did not
retain enough stage-level logs to quantify each component.

The fixed actor workflow timeout is independently defective for large actors:
the actor lock has a 30-second TTL and the workflow is cancelled after 28
seconds. The measured 2 GiB checkpoint is already near this limit, and the 4
GiB case cannot fit within it. Extending the timeout alone is not a performance
optimization, but a lease-safe timeout design is required to measure the large
cases correctly.

## Chosen Strategy

Use a measurement-gated sequence with three independently switchable variants:

1. **Baseline: compressed Fluid snapshot.** Preserve the current sparse-zstd
   behavior and add complete stage telemetry.
2. **Raw Fluid snapshot.** Atomically publish immutable raw sparse snapshot
   files in Fluid. Restore them through a local materialization path first, then
   test direct runsc access to the immutable Fluid directory if read-only
   behavior is confirmed.
3. **Decoded node cache.** If raw Fluid access is not superior, cache decoded
   immutable snapshots locally by manifest digest for repeated golden-snapshot
   restores.

The variants are runtime-configurable so the same image can run randomized A/B
tests. The implementation will keep the compressed format as the compatibility
default until a variant passes correctness, reliability, and performance gates.

This strategy is preferred over additional OCI work because a cache-hit OCI
clone averages about 26 ms, which is negligible beside a 9--17 second memory
restore. It is preferred over a timeout-only change because timeout changes do
not reduce latency. It excludes incremental checkpointing, demand paging, and
lazy restore because those require gVisor changes.

## Critical-Path Telemetry

Every lifecycle operation will emit one trace-correlated summary and per-file
records. Durations use monotonic clocks and byte counters are taken from the
actual files or streams.

Checkpoint stages:

- `runsc_checkpoint`
- `runtime_cleanup`
- `snapshot_enumeration`
- `sparse_scan`
- `compression`
- `fluid_write`
- `sync_publish`
- `manifest_publish`
- `total`

Restore stages:

- `manifest_read`
- `fluid_open_read`
- `decompression`
- `local_materialization`
- `oci_bundle`
- `network_setup`
- `runsc_pause_restore`
- `runsc_app_restore`
- `readiness_wait`
- `total`

Each record includes actor/template, node, snapshot URI, backend, format,
cache status, logical bytes, populated bytes, stored bytes, bytes processed per
second, and error stage. Telemetry failures must never fail an actor operation.

## Storage Formats and Atomicity

### Compressed format

The existing `.zstd` sparse-extent files remain readable and writable. Their
manifest explicitly records format version `sparse-zstd-v1` rather than relying
only on filename suffixes.

### Raw sparse format

Raw runsc snapshot files are copied into a unique temporary Fluid directory.
The writer preserves logical size and holes where the filesystem supports them.
After all files have been closed and synced, it writes a versioned manifest and
atomically renames or publishes the directory. Restore never consumes a raw
snapshot before the manifest is visible.

The manifest records file name, logical size, allocated size, and digest. A
restore validates paths and sizes before passing the directory to runsc.
Direct restore treats published files as immutable. If a test proves that
runsc modifies any file, direct mode is rejected and only local
materialization remains eligible.

### Decoded cache

The optional cache key is the cryptographic digest of the manifest plus file
metadata. A per-key single-flight lock prevents duplicate fills. Cache entries
are written to a temporary directory and atomically published. Entries are
read-only, capacity-bounded, and evicted by least-recent use. Corrupt entries
are removed and rebuilt; restore falls back to the source snapshot.

## Workflow Lease and Timeout

Large snapshots use a renewable actor-operation lease rather than one fixed
30-second lock. Renewal is conditional on ownership of the original lock
token. Losing ownership cancels the operation. The operation deadline is a
separate configurable safety limit, recorded in telemetry, and is not used as
a performance target.

Initial tests use a sufficiently high safety deadline to observe completion.
After measurements, production defaults can be derived from measured stage
distributions and snapshot size. Cancellation must terminate child runsc and
storage work, release or expire the lease, and leave the actor recoverable.

## Benchmark Workloads

The memory workload gains explicit profiles:

- `sparse-randomish`: current two-touched-bytes-per-page behavior, retained for
  comparison;
- `zeroed`: compression upper bound;
- `dense-random`: deterministic pseudorandom content across every page,
  representing the incompressible bound.

Every sample records requested bytes, resident bytes, profile, seed, and
checksum. A sample is valid only when the workload reports readiness and the
resident allocation is at least 95% of the target.

## Experiment Design

### Diagnostic runs

Use 64, 256, and 512 MiB first. For each format/profile pair, execute three
independent rounds of at least 20 actors at concurrency one. Use an A-B-B-A
ordering between current compressed mode and the candidate to reduce temporal
cluster bias. Report every round separately and aggregated.

The diagnostic decision uses stage-level evidence:

- Continue raw Fluid work only if it removes compression/materialization time
  without transferring a larger cost into Fluid I/O or runsc restore.
- Enable direct restore only if snapshot immutability and correctness tests
  pass and it improves end-to-end latency over raw local materialization.
- Develop decoded caching only if repeated-snapshot scenarios have measurable
  reuse and raw Fluid is not the winning first-restore path.

### Large-memory reliability runs

After lease renewal is validated, run 1 GiB and 2 GiB. A 4 GiB smoke run is
allowed only after 2 GiB completes without stranded actors. Failed samples are
included in error-rate reporting and excluded from latency percentiles only
with an explicit count and reason.

### Confirmation runs

The winning configuration receives at least 100 successful samples per memory
bucket for a stable P95 comparison. P99 is labelled exploratory until a bucket
has at least 1,000 successful restores, matching the repository performance
test plan. Comparisons include bootstrap confidence intervals or round-level
dispersion, not only pooled averages.

## Success and Stop Rules

No arbitrary percentage is a prerequisite. A candidate is accepted only when:

- its intended stage becomes measurably faster;
- end-to-end P50 and P95 improve in the same direction across independent
  rounds;
- error rate does not increase;
- checksum/readiness verification succeeds after restore;
- CPU, allocated storage, and network amplification are reported;
- no actor remains indefinitely in a transitional state.

A candidate is stopped when the supposedly removed stage is not material,
another stage absorbs the savings, or operational cost is disproportionate to
the repeatable end-to-end gain. The final report states both wins and rejected
hypotheses.

## Safety and Rollback

All new behavior is feature-gated. The current sparse-zstd format remains the
default and reader compatibility is retained. Test namespaces and snapshot
prefixes are unique per run. Before and after each round, the harness records
DaemonSet configuration, actor residues, worker readiness, image digests, and
storage backend status. On failure, the test restores the prior DaemonSet
configuration and deletes only resources created by its run ID.

The repository is already dirty from earlier work. Implementation commits will
stage only files belonging to this design and will not revert unrelated user
changes.

## Deliverables

- stage telemetry and report schema;
- runtime-selectable compressed, raw-materialized, and eligible direct-restore
  paths;
- renewable workflow lease for long snapshot operations;
- realistic memory workload profiles;
- raw JSONL/log evidence for every round;
- per-round and aggregate CSV/HTML reports;
- a final decision report showing key-path changes, rejected variants,
  reliability boundaries, and environmental configuration.
