# Checkpoint Storage Optimization Design

## Objective

Reduce Substrate checkpoint latency without changing gVisor. The work focuses
on the checkpoint storage path after `runsc checkpoint`: sparse scan,
compression, Fluid/JindoFS writes, sync, and manifest publication. All
performance claims must come from raw lifecycle JSONL and structured atelet
stage logs.

## Current Evidence

Existing snapshot critical-path data shows that `runsc checkpoint` is not the
checkpoint bottleneck:

| Memory | runsc checkpoint | snapshot storage |
|---:|---:|---:|
| 64 MiB | ~148 ms | ~2.88 s |
| 256 MiB | ~225 ms | ~4.56 s |
| 512 MiB | ~346 ms | ~6.99 s |

For 64 MiB, one observed `pages.img` compression duration was about 85 ms,
while the combined write path for the same file was about 2.55 s. This points
at Fluid/JindoFS write, sync, or publish behavior rather than zstd CPU time.

Raw Fluid snapshots are already rejected for this environment: a 64 MiB raw
checkpoint exceeded 28 seconds and produced multiple timeouts. That path is
not a checkpoint optimization candidate unless new evidence invalidates the
previous run.

## Strategy

Use a measurement-gated sequence:

1. Add fine-grained checkpoint storage telemetry while preserving the current
   `sparse-zstd-v1` format.
2. Run a memory-size validation matrix from 64 MiB through 16 GiB, subject to
   safety gates for large sizes.
3. Optimize the existing write path only where telemetry shows material cost.
4. If the dominant bottleneck remains one large `pages.img` write, add a new
   chunked snapshot format as a separate candidate.

The first implementation must not change checkpoint durability semantics:
external snapshots remain usable only after all data files and the manifest are
published. Manifest visibility continues to mean the snapshot is complete.

## Telemetry Requirements

Each checkpoint emits one operation summary and per-file stage records.

Per-file fields:

- actor, template, node, trace ID
- snapshot format and backend
- file name
- logical bytes
- populated bytes
- compressed bytes
- sparse scan duration
- compression duration
- storage write duration
- close duration
- file sync duration, when available
- total file duration
- error stage and error reason

Operation-level fields:

- runsc checkpoint duration
- snapshot enumeration duration
- summed logical/populated/compressed bytes
- storage wall time
- manifest write duration
- manifest sync/publish duration, when available
- total checkpoint duration
- failed stage

Telemetry failures must never fail actor checkpoint. If telemetry cannot be
collected, the operation logs a warning and continues.

## Phase 1: Existing Format Optimization

Phase 1 keeps `sparse-zstd-v1` compatible with existing restore code.

Allowed optimizations:

- make sparse scan, compression, write, close, and sync timing explicit;
- increase buffered write size when current writes are small;
- ensure file-level workers are actually parallel where multiple snapshot
  files exist;
- avoid redundant sync calls that do not strengthen manifest-last durability;
- keep manifest publication last;
- preserve existing fallback behavior.

Disallowed optimizations in Phase 1:

- returning from `PauseActor` before the external snapshot is durable;
- changing runsc flags;
- changing gVisor;
- making local cache the source of truth;
- making raw Fluid snapshots default.

## Phase 2: Chunked Candidate

If Phase 1 evidence shows `pages.img` is the long pole, introduce
`sparse-zstd-chunked-v2` as an opt-in format.

The v2 writer splits large files into fixed-size chunks. Each chunk records:

- source file name
- logical offset
- logical size
- populated bytes
- compressed bytes
- digest
- object path

Chunks are written and verified before the manifest is published. Restore
reassembles chunks into the local runsc checkpoint file and validates size and
digest. The v1 reader remains the default compatibility path until v2 wins
repeated A/B validation.

## Validation Matrix

Memory buckets:

- 64 MiB
- 256 MiB
- 512 MiB
- 1 GiB
- 2 GiB
- 4 GiB
- 8 GiB
- 16 GiB

Large-size safety gates:

- Run 1 GiB and 2 GiB before 4 GiB.
- Run 4 GiB before 8 GiB.
- Run 8 GiB before 16 GiB.
- Do not continue to the next size if the previous size leaves residual
  actors, lock ownership errors, crashed workers, or failed cleanup.
- Do not claim P99 below 1,000 successful samples per bucket.
- Use small diagnostic counts first, then confirmation counts only for
  candidates that repeat.

Every round must preserve:

- raw lifecycle JSONL;
- atelet structured stage logs;
- deployed image digests and DaemonSet environment;
- node free disk before and after large-memory runs;
- failed samples and cleanup errors.

## Acceptance Criteria

A checkpoint optimization candidate is accepted only when:

- the intended checkpoint sub-stage gets measurably faster;
- end-to-end `suspend_1` and `suspend_2` improve across independent rounds;
- error rate does not increase;
- external restore from the produced snapshot still succeeds;
- no actor remains indefinitely transitional;
- CPU, storage, and network amplification are reported.

If the data shows checkpoint time is dominated by Fluid/JindoFS sync behavior
that cannot be improved safely in Substrate, the candidate is rejected and the
report must say so explicitly.

## Rollback

All new behavior is feature-gated. The existing `sparse-zstd-v1` path remains
the default until evidence supports changing it. Pre-test cluster YAML and
image digests are retained so ateapi and atelet can be restored after
experiments.
