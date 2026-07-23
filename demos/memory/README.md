# Memory Gradient Demo

This demo is a Substrate workload for snapshot/restore performance testing.
It allocates a configurable amount of resident memory, touches every page, and
keeps serving HTTP readiness and status endpoints so checkpoint/restore tests
can verify the actor is alive.

Set `TARGET_MEMORY_MIB` on the `ActorTemplate` container to control the target
resident memory size.
