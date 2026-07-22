# Hot Migration Design

## Goal

Implement request-level hot migration for Substrate actors so an actor can move from one worker node to another while the stable router endpoint remains reachable. The first production target is HTTP/API continuity, not preservation of the original TCP connection.

The success target for the first end-to-end benchmark is:

| Metric | Target |
|---|---:|
| Source and target placement | Different Kubernetes nodes |
| HTTP 5xx during migration | 0 after router retry policy |
| Brownout visible to client | p95 <= 3s |
| Route switch | p95 <= 100ms |
| Start migration to target serving | p95 <= 30s |
| State check | Counter/checksum/generation does not regress |

The current measured baseline for a 16GiB cross-node restore is useful but not hot migration: suspend/checkpoint is about 12.167s, cross-node recover is about 1.024s, and the pure migration path excluding test blocker boot is about 13.412s. The main design objective is to move heavy checkpoint/restore work out of the request interruption window.

## Non-Goals

- Do not preserve an existing TCP four-tuple across nodes in the first version.
- Do not promise lossless non-idempotent writes for workloads that do not expose a quiesce contract.
- Do not replace the router with a service mesh or require BeeGFS/Fluid as a mandatory dependency.
- Do not implement active-active application replication in the generic runtime layer.

## Architecture

The design adds a make-before-break migration path:

1. Keep the source worker serving.
2. Allocate a target worker on a different node.
3. Restore the target from the latest available snapshot while the source still serves traffic.
4. Probe the target.
5. Drain source traffic at the router.
6. Quiesce the source for a short finalization window.
7. Produce/apply the final state.
8. Atomically switch the actor route to the target.
9. Keep the source for a rollback grace period, then release it.

The current router path calls `ResumeActor` for each routed actor and forwards to the returned `Actor.ateom_pod_ip`. Hot migration needs a separate route decision so the control plane can keep both source and candidate target alive at the same time.

## API Model

Add control-plane RPCs:

- `PrepareActorMigration`: allocate target worker and restore a candidate target from the actor's latest snapshot.
- `CommitActorMigration`: drain, final checkpoint, target refresh, route switch, source cleanup.
- `AbortActorMigration`: release the candidate target and leave routing on the source.
- `GetActorRoute`: return the router-visible route state for an actor.

Add actor fields:

- `route`: active and candidate route targets, phase, generation, and drain deadline.
- `migration`: migration id, source, target, base snapshot, final snapshot, and observed stage timings.

These fields live on the existing `Actor` proto initially. That keeps optimistic concurrency and existing Redis actor storage as the source of truth. A separate route key can be introduced later if route write volume becomes a bottleneck.

## Route Phases

| Phase | Router behavior |
|---|---|
| `ROUTE_PHASE_ACTIVE` | Route all traffic to `route.active`. |
| `ROUTE_PHASE_PREPARING` | Route all traffic to source; target is restoring/probing. |
| `ROUTE_PHASE_DRAINING` | Stop sending new requests to source. Retry or wait for target depending on request method and deadline. |
| `ROUTE_PHASE_SWITCHED` | Route all new traffic to target. Source remains reserved for rollback grace. |
| `ROUTE_PHASE_ROLLBACK` | Route all traffic to source and release target. |

The route generation is incremented for every switch. Router logs and benchmark probes include the generation to prove the target is serving after migration.

## Router Behavior

The router gains an `ActorRouteResolver`:

- If the actor has an active route, use it.
- If the actor is suspended, call `ResumeActor` and then route to the resumed worker.
- If the actor is migrating, follow `route.phase` instead of blindly using `Actor.ateom_pod_ip`.

The direct HTTP proxy gains per-actor in-flight accounting:

- Increment before forwarding.
- Decrement after the response finishes.
- During drain, wait for existing source in-flight requests up to the configured drain deadline.
- New idempotent requests can wait briefly and retry against the switched target.
- Non-idempotent requests require app quiesce for strong correctness; without a quiesce hook, the router only provides retry-level availability.

Envoy ext_proc gets the same route resolution logic for header mutation, but direct proxy is the first test target because it was already used for the current cross-node continuity benchmark.

## Workload Quiesce Contract

The generic workload contract is optional:

- `POST /substrate/quiesce`: stop accepting mutating work and flush in-memory state.
- `POST /substrate/resume`: resume normal work after rollback or failed migration.
- `GET /substrate/migration-state`: return a generation/checksum/counter summary for validation.

If the hooks exist, control plane calls them before final checkpoint. If they do not exist, migration is still allowed in best-effort mode for idempotent/retryable HTTP workloads. Benchmarks must label the result as `quiesce=enabled` or `quiesce=disabled`.

## Final State Strategy

Version 1 uses a final external checkpoint during the finalization window. This is enough to measure the real brownout and prove the route-control architecture.

Version 2 reduces finalization time using one of these mechanisms:

- runtime dirty-page delta if available for the sandbox class;
- application-level compact state checkpoint through the quiesce hook;
- periodic background checkpoint so the final checkpoint only captures a small delta.

The design does not assume that a 16GiB full memory image can be scanned in the cutover window. If final checkpoint remains near 12s, the next optimization target is dirty tracking or app-level compact state, not storage backend replacement.

## Rollback

Rollback is allowed until the source release step:

- If target restore or probe fails, keep route on source and release target.
- If commit fails before route switch, call `/substrate/resume` on source when quiesce was enabled.
- If post-switch probe fails during rollback grace, switch route back to source and release target.
- Once source is released, rollback is no longer available; failures become normal actor recovery.

## Observability

Each migration emits stage timings:

- `prepare_allocate_worker_ms`
- `prepare_restore_ms`
- `target_probe_ms`
- `drain_wait_ms`
- `source_quiesce_ms`
- `final_checkpoint_ms`
- `target_final_restore_ms`
- `route_switch_ms`
- `post_switch_probe_ms`
- `source_release_ms`

The benchmark result must include:

- source node and target node;
- actor route generation before and after;
- request count, status code distribution, and retry count;
- longest client-visible gap between successful responses;
- checksum/counter before and after;
- total time from migration RPC start to first successful target response.

## Confidence

| Scope | Confidence after implementation |
|---|---:|
| HTTP request-level continuity with quiesce hook | 0.90-0.93 |
| HTTP request-level continuity without quiesce, idempotent requests only | 0.75-0.82 |
| WebSocket continuity with reconnect/resume support | 0.65-0.75 |
| Original TCP connection preservation across nodes | 0.20-0.35 |

The confidence increase comes from removing full restore from the cutover path, adding router drain, adding an explicit route generation, and validating under continuous load.

