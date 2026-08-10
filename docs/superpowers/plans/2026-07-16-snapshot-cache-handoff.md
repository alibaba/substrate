# Snapshot Performance Optimization Handoff

Date: 2026-07-16 17:45 Asia/Shanghai

## User Goal and Hard Constraints

- Automatically investigate, design, implement, and repeatedly validate a substantial snapshot performance improvement.
- Do not assume an improvement percentage; every claim must be derived from raw JSONL and component timing logs.
- Do not modify gVisor. Changes may be made only in Substrate, storage paths, scheduling, caching, and benchmark tooling.
- Preserve failed samples and invalid experiments; do not hide errors.
- The workspace is dirty. Do not reset or overwrite unrelated existing changes.
- Cluster kubeconfig: `/tmp/ack-fluid.kubeconfig`.
- Local SOCKS5 proxy: `127.0.0.1:5003`.

## Existing Evidence and Decisions

Primary result directory:

`results/snapshot-critical-path-20260716T150500Z/`

Valid sparse-zstd baseline raw data:

- `raw/substrate-api/baseline-sparse-64m-r2.jsonl`
- `raw/substrate-api/baseline-sparse-256m-r2.jsonl`
- `raw/substrate-api/baseline-sparse-512m-r2.jsonl`

Measured averages:

| Memory | checkpoint runsc | checkpoint storage | restore download/decode | ateom/runsc restore | restore total |
|---|---:|---:|---:|---:|---:|
| 64 MiB | 148 ms | 2.88 s | 650 ms | 238 ms | 901 ms |
| 256 MiB | 225 ms | 4.56 s | 2.43 s | 1.90 s | 4.36 s |
| 512 MiB | 346 ms | 6.99 s | 4.81 s | 4.79 s | 9.66 s |

Conclusions supported by data:

- OCI unpack is about 26 ms and is not the bottleneck.
- Checkpoint is dominated by compressed external storage.
- At 512 MiB, restore is split approximately evenly between download/decode and gVisor restore.
- Because gVisor cannot be changed, eliminating download/decode is the largest measured available restore opportunity.

Rejected candidates:

1. `raw-sparse-v1` plus direct Fluid restore: rejected. A 64 MiB raw checkpoint exceeded 28 seconds and seven samples timed out at `suspend_1`. Preserve:
   - `raw/substrate-api/raw-direct-64m-r1.jsonl`
   - `raw/substrate-api/raw-direct-256m-r1.jsonl`
   - `raw/substrate-api/raw-direct-512m-r1.jsonl`
2. zstd decoder concurrency 4: technically works but end-to-end warm resume improved only about 3.0%-3.7%, insufficient as the main optimization. Preserve `decoder4-*-r1.jsonl`.

## Previously Completed Implementation

- Structured atelet checkpoint and restore critical-path timings.
- ateom-gvisor runtime stage timings without changing runsc behavior.
- Versioned sparse/raw snapshot formats and direct raw experiment.
- Renewable ateapi workflow lock lease and separate operation timeout.
- Deterministic memory profiles (`zeroed`, `sparse-randomish`, `dense-random`).
- zstd decoder concurrency option.
- Perfkit/raw result infrastructure.

Design commit already present:

`cec10e0a docs: design snapshot critical-path optimization`

Main documents:

- `docs/superpowers/specs/2026-07-16-snapshot-critical-path-optimization-design.md`
- `docs/superpowers/plans/2026-07-16-snapshot-critical-path-optimization.md`

## Node-Local Decoded Cache Candidate Implemented in This Session

The implementation plan was extended with Task 11 in:

`docs/superpowers/plans/2026-07-16-snapshot-critical-path-optimization.md`

New files:

- `cmd/atelet/snapshot_cache.go`
- `cmd/atelet/snapshot_cache_test.go`

Modified for this candidate:

- `cmd/atelet/main.go`
- `internal/ateompath/ateompath.go`
- `internal/proto/ateletpb/atelet.proto` and generated `atelet.pb.go`
- `pkg/proto/ateapipb/ateapi.proto` and generated `ateapi.pb.go`
- `cmd/ateapi/internal/controlapi/workflow_suspend.go`
- `cmd/ateapi/internal/controlapi/workflow_suspend_test.go`
- `cmd/ateapi/internal/controlapi/workflow_resume.go`
- `cmd/ateapi/internal/controlapi/workflow_resume_test.go`

Implemented behavior:

- Feature gate: `ATE_SNAPSHOT_CACHE_MAX_BYTES`; absent, invalid, zero, or negative disables the cache.
- External sparse-zstd snapshot remains the portable source of truth.
- After the compressed external snapshot and manifest are durable, decoded checkpoint files are moved on the same filesystem into a temporary cache entry and atomically renamed. There is no second large-file copy.
- Cache keys are SHA-256 hashes of normalized snapshot URI prefixes.
- Cache lookup requires an exact manifest match and validates every expected regular file.
- Cache hit replaces the actor `restore-state` directory with a symlink to the immutable cache entry.
- Cache miss or cache lookup error preserves/falls back to the existing external download and decode path.
- Restore timing log now includes `snapshot_cache_hit`.
- Entries referenced by actor `restore-state` symlinks cannot be evicted.
- Capacity is bounded; oldest unreferenced entries are evicted. If all possible victims are referenced, the new entry is discarded and the external snapshot remains usable.
- A macOS `/var` to `/private/var` path-alias bug in reference detection was found by tests and fixed by comparing resolved roots.
- `CheckpointResponse.local_cache_published` reports actual successful publication.
- `ExternalSnapshotInfo.node_vms_with_local_cache` stores cache node hints.
- External cache locality is a soft scheduling preference. If the cached node has no eligible free worker, selection falls back to another eligible node. Existing Pause local-snapshot locality remains a hard restriction.

Important lifecycle reasoning:

- gVisor background restore can continue reading `restore-state` after restore returns.
- The cache entry stays referenced through the `restore-state` symlink and therefore cannot be evicted.
- On a later checkpoint, gVisor checkpoint runs before `resetActorDirs`; it pages in anything still needed. Cleanup then removes only the symlink, not the cache entry.

## Tests Completed

Focused cache tests cover:

- URI isolation and immutable re-publication.
- Atomic complete-entry publication.
- Path traversal rejection.
- Incomplete-entry clean miss.
- Bounded eviction.
- Referenced-entry eviction protection.
- Hit symlink and miss directory preservation.
- Explicit/valid cache configuration.
- Soft locality preference and portable fallback.
- Recording the originating cache node after suspend.

Latest successful verification:

```bash
go test ./cmd/atelet/... \
  ./cmd/ateapi/internal/controlapi \
  ./cmd/ateapi/internal/store/ateredis \
  ./demos/memory \
  ./benchmarking/perfkit/... -count=1

GOOS=linux GOARCH=amd64 go test -c ./cmd/ateom-gvisor -o /tmp/ateom-gvisor.test
git diff --check
```

All passed. The controlapi suite took about 23 seconds.

## Cluster State at Handoff

Current atelet DaemonSet before cache deployment:

- Image: `192.168.53.189:5000/substrate/atelet-89dbecdd4e8d5cd4d125a2de341f399c:decoder-concurrency-20260716T1704Z`
- `ATE_SNAPSHOT_FORMAT=sparse-zstd-v1`
- `ATE_SNAPSHOT_DIRECT_RESTORE=0`
- `ATE_SNAPSHOT_DECODER_CONCURRENCY=4`
- 10/10 atelet pods Ready.

Current ateapi is still the older deployed image:

`test-yuanxun-registry.cn-hangzhou.cr.aliyuncs.com/substrate/ateapi-752889f8b0bcdbee32172ac9fe056025@sha256:a7198e...`

The previously built renewable-lease ateapi image has digest:

`sha256:8b57c41895549752c6135f8d22b765979273b0e0d412e4055fa6c128d4e00efe`

Memory workload:

- Namespace: `ate-demo-memory-gradient`
- Deployment: `memory-gradient-deployment`, 8/8 Ready.
- Templates ready: 64 MiB, 256 MiB, 512 MiB, 1 GiB, 2 GiB.

Temporary resources still present and must eventually be deleted:

- `ate-system/image-pusher`
- `ate-system/prepull-snapshot-atelet`
- `ate-system/prepull-snapshot-ateom`
- `ate-system/prepull-decoder-atelet`

Pre-cache cluster snapshots were captured locally:

- `/tmp/pre-cache-atelet.yaml`
- `/tmp/pre-cache-ateapi.yaml`

## Exact Current Blocker / Last Failed Command

No cache image has been deployed yet.

Two attempts to create image tarballs failed because of ko publishing semantics, not compilation:

1. `KO_DOCKER_REPO=ko.local/... --tarball=...` tried to load the image into a nonexistent local Docker daemon.
2. `KO_DOCKER_REPO=cache.invalid/... --tarball=...` successfully wrote the atelet tar, then still attempted registry access and failed DNS resolution. Adding `--sbom=none` did not disable the final registry access.

The next attempt should use ko's explicit no-push mode:

```bash
KO_DOCKER_REPO=cache.invalid/substrate ko build ./cmd/atelet \
  --platform=linux/amd64 --bare --tags=cache-20260716 \
  --sbom=none --push=false \
  --tarball=/tmp/snapshot-cache-build/atelet.tar

KO_DOCKER_REPO=cache.invalid/substrate ko build ./cmd/ateapi \
  --platform=linux/amd64 --bare --tags=cache-20260716 \
  --sbom=none --push=false \
  --tarball=/tmp/snapshot-cache-build/ateapi.tar
```

If that ko version still refuses, use `--oci-layout-path` and let cluster-side crane copy from an OCI layout/archive. Do not retry local Docker or slow local port-forward pushes.

Established upload path:

1. Split/copy tar content to `ate-system/image-pusher` with `kubectl cp`.
2. Run crane inside the cluster to push to `192.168.53.189:5000/substrate/...`.
3. Use a prepull DaemonSet because node containerd treats the HTTP registry as HTTPS.

## Next Steps

1. Produce and push new atelet and ateapi images; record immutable digests.
2. Prepull images on the ten real atelet/worker nodes.
3. Deploy ateapi first (proto is backward compatible), then cache atelet with:
   - `ATE_SNAPSHOT_FORMAT=sparse-zstd-v1`
   - `ATE_SNAPSHOT_DIRECT_RESTORE=0`
   - choose an explicit bounded `ATE_SNAPSHOT_CACHE_MAX_BYTES` (for example enough for the planned 8 workers and 512 MiB logical snapshots; record the chosen value and node free disk first).
4. Run a smoke actor and verify:
   - checkpoint response leads to an external snapshot node hint;
   - resume is scheduled to the hinted node;
   - atelet log has `snapshot_cache_hit=true`;
   - download duration approaches zero;
   - external fallback works after deliberately deleting one cache entry or selecting a noncached node.
5. Execute interleaved cache-off/cache-on A/B rounds for 64, 256, and 512 MiB. Preserve raw JSONL and errors. At least three independent rounds before deciding.
6. Compare end-to-end warm resume, download/decode, ateom restore, hit rate, fallback count, and failures. Do not claim P99 with fewer than 1,000 successes.
7. If cache gains repeat, run confirmation samples and then 1/2 GiB lease tests. If not, reject it with raw evidence.
8. Restore pre-test cluster configuration and delete all temporary prepull/image-pusher resources and residual actors.

## Cautions for the Next Model

- The repository is on `main` with many pre-existing uncommitted changes. Do not reset or checkout files.
- Do not remove the rejected raw/direct experiment data.
- Do not alter runsc flags or gVisor source.
- Cache publication errors are intentionally nonfatal because the external snapshot is already durable.
- Do not turn external cache hints into hard restrictions.
- Before claiming success, run fresh verification and base the performance decision on raw cluster data, not unit tests.
