# Hot Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and verify L7 request-level hot migration for Substrate actors with cross-node target placement, stable router access, request drain, rollback, and measured end-to-end data.

**Architecture:** The control plane keeps source and target workers alive during migration, stores route state on the actor, and switches traffic through the router after target restore and drain. The router resolves an actor route separately from `ResumeActor`, tracks in-flight requests, and uses route generation to prove traffic moved to the target. The first implementation uses a final checkpoint in the cutover window, then measures whether dirty-delta work is required.

**Tech Stack:** Go, gRPC/protobuf, Kubernetes workers, existing `ActorWorkflow`, Redis-backed store, `atenet` direct HTTP proxy/ext_proc router, perfkit benchmark harness.

---

## Current Constraints

- Existing `ResumeActor` has a single active worker model.
- Existing router calls `ResumeActor` on request path and forwards to `Actor.ateom_pod_ip`.
- Existing `SuspendActor` releases the source worker; hot migration must not use it directly for the prepare phase.
- Current 16GiB data shows restore is fast enough, but final checkpoint can be about 12s if it scans the full memory image.

## File Structure

| File | Responsibility |
|---|---|
| `pkg/proto/ateapipb/ateapi.proto` | Public migration RPCs and actor route/migration state. |
| `pkg/proto/ateapipb/ateapi.pb.go` | Generated Go proto. |
| `pkg/proto/ateapipb/ateapi_grpc.pb.go` | Generated gRPC service bindings. |
| `cmd/ateapi/internal/store/store.go` | Store interface additions only if route gets a separate key; version 1 keeps route on `Actor`, so no new store methods required. |
| `cmd/ateapi/internal/controlapi/migrate_actor.go` | `Service` RPC handlers for prepare, commit, abort, and get route. |
| `cmd/ateapi/internal/controlapi/workflow_migrate.go` | Migration workflow orchestration and steps. |
| `cmd/ateapi/internal/controlapi/workflow_migrate_test.go` | Unit tests for migration state transitions and rollback. |
| `cmd/ateapi/internal/controlapi/worker_selection.go` | Shared worker selection helpers if extracted from resume workflow. |
| `cmd/atenet/internal/router/route_resolver.go` | Router-side route lookup and fallback resume behavior. |
| `cmd/atenet/internal/router/route_resolver_test.go` | Unit tests for route decisions. |
| `cmd/atenet/internal/router/inflight.go` | Per-actor in-flight accounting and drain wait. |
| `cmd/atenet/internal/router/inflight_test.go` | Unit tests for drain wait and timeout behavior. |
| `cmd/atenet/internal/router/direct_proxy.go` | Use route resolver and in-flight accounting. |
| `cmd/atenet/internal/router/extproc.go` | Use route resolver for Envoy header mutation. |
| `demos/memory/hot_migration_server.go` | HTTP demo workload with counter, checksum, generation, and quiesce hooks. |
| `benchmarking/perfkit/internal/runner/hot_migration.go` | End-to-end benchmark runner. |
| `benchmarking/perfkit/internal/runner/hot_migration_test.go` | Benchmark parser/unit tests. |
| `docs/hot-migration-runbook.md` | How to run the 16GiB cross-node hot migration benchmark and interpret results. |

## Task 0: Confidence Burn-Down Before Broad Implementation

**Preflight result on 2026-07-22:** Steps 1-4 were checked against the current workspace. Proto generation uses `pkg/proto/ateapipb/gen.go` and `internal/proto/ateletpb/gen.go`; control API handlers use `*Service`; `functional_test.go` has a fake atelet with `Checkpoint` and `Restore` request capture; direct HTTP proxy exists behind `--direct-http-proxy`. The remaining confidence work is the dual-worker functional test required in Step 5.

**Files:**
- Read: `pkg/proto/ateapipb/gen.go`
- Read: `internal/proto/ateletpb/gen.go`
- Read: `cmd/ateapi/internal/controlapi/service.go`
- Read: `cmd/ateapi/internal/controlapi/functional_test.go`
- Read: `cmd/atenet/internal/router/direct_proxy.go`
- Modify: `docs/superpowers/plans/2026-07-22-hot-migration.md` only if a checked assumption is false

- [ ] **Step 1: Verify proto generation commands**

Run:

```bash
rg -n "go:generate" pkg/proto/ateapipb internal/proto/ateletpb
```

Expected output includes:

```text
pkg/proto/ateapipb/gen.go:... go:generate ... ateapi.proto
internal/proto/ateletpb/gen.go:... go:generate ... atelet.proto
```

If either line is missing, stop and update Task 1 or Task 10 before changing proto files.

- [ ] **Step 2: Verify control API receiver and test harness**

Run:

```bash
rg -n "type Service|UnimplementedControlServer|RegisterControlServer|FakeAteletServer|func \\(s \\*Service\\) ResumeActor" cmd/ateapi/internal/controlapi -S
```

Expected output proves:

```text
cmd/ateapi/internal/controlapi/service.go:... type Service struct
cmd/ateapi/internal/controlapi/service.go:... ateapipb.UnimplementedControlServer
cmd/ateapi/internal/controlapi/functional_test.go:... type FakeAteletServer struct
cmd/ateapi/internal/controlapi/resume_actor.go:... func (s *Service) ResumeActor
```

If the receiver is not `*Service`, update all RPC handler snippets before implementation.

- [ ] **Step 3: Verify dual-worker test can be built on existing fake atelet**

Run:

```bash
sed -n '150,240p' cmd/ateapi/internal/controlapi/functional_test.go
```

Expected: `FakeAteletServer` has both `Checkpoint` and `Restore`, and stores the last request. This is required for a high-confidence prepare/commit functional test.

- [ ] **Step 4: Verify router direct proxy is available for the first e2e test**

Run:

```bash
rg -n "DirectHTTPProxy|serveDirectHTTPProxy|handleDirectProxy" cmd/atenet/internal/router.go cmd/atenet/internal/router -S
```

Expected output includes `DirectHTTPProxy`, `serveDirectHTTPProxy`, and `handleDirectProxy`. If direct proxy is missing, implement it before hot migration routing because Envoy image availability previously blocked testing.

- [ ] **Step 5: Add a minimum functional-test requirement before coding Task 3**

Before Task 3 is considered complete, the implementation must include a functional test with this shape:

```go
func TestPrepareActorMigrationKeepsSourceAndRestoresTarget(t *testing.T) {
	// Create actor, create two workers on different nodes, resume actor on worker A.
	// Call PrepareActorMigration(require_cross_node=true).
	// Assert worker A remains assigned to the actor as route.active.
	// Assert worker B is assigned to the actor as route.candidate.
	// Assert fake atelet Restore was called for worker B.
	// Assert actor route phase is PREPARING or TARGET_READY and HTTP route still points at worker A.
}
```

- [ ] **Step 6: Commit any plan corrections**

If Steps 1-4 reveal a mismatch, edit this plan before implementation and commit:

```bash
git add docs/superpowers/plans/2026-07-22-hot-migration.md
git commit -m "docs: tighten hot migration confidence gates"
```

If no mismatch is found, do not create an empty commit.

## Task 1: Add Proto API and Route State

**Files:**
- Modify: `pkg/proto/ateapipb/ateapi.proto`
- Modify: `pkg/proto/ateapipb/ateapi.pb.go`
- Modify: `pkg/proto/ateapipb/ateapi_grpc.pb.go`

- [ ] **Step 1: Edit `pkg/proto/ateapipb/ateapi.proto`**

Add these RPCs to `service Control` after `ResumeActor`:

```proto
  // Prepare a target worker for request-level hot migration while the source keeps serving.
  rpc PrepareActorMigration(PrepareActorMigrationRequest) returns (PrepareActorMigrationResponse) {}

  // Commit a prepared migration by draining the source, finalizing state, and switching the route.
  rpc CommitActorMigration(CommitActorMigrationRequest) returns (CommitActorMigrationResponse) {}

  // Abort a prepared migration and keep the actor routed to its source worker.
  rpc AbortActorMigration(AbortActorMigrationRequest) returns (AbortActorMigrationResponse) {}

  // Return the router-visible route state for an actor.
  rpc GetActorRoute(GetActorRouteRequest) returns (GetActorRouteResponse) {}
```

Add these fields to `message Actor` after `worker_pool_name = 12`:

```proto
  ActorRoute route = 13;
  ActorMigration migration = 14;
```

Add these messages after `message Actor`:

```proto
message RouteTarget {
  string ateom_pod_namespace = 1;
  string ateom_pod_name = 2;
  string ateom_pod_ip = 3;
  string ateom_pod_uid = 4;
  string worker_pool_name = 5;
  string node_name = 6;
}

message ActorRoute {
  enum Phase {
    PHASE_UNSPECIFIED = 0;
    PHASE_ACTIVE = 1;
    PHASE_PREPARING = 2;
    PHASE_DRAINING = 3;
    PHASE_SWITCHED = 4;
    PHASE_ROLLBACK = 5;
  }

  RouteTarget active = 1;
  RouteTarget candidate = 2;
  Phase phase = 3;
  int64 generation = 4;
  google.protobuf.Timestamp drain_deadline = 5;
}

message ActorMigration {
  enum Phase {
    PHASE_UNSPECIFIED = 0;
    PHASE_PREPARING = 1;
    PHASE_TARGET_READY = 2;
    PHASE_DRAINING = 3;
    PHASE_FINALIZING = 4;
    PHASE_SWITCHED = 5;
    PHASE_COMMITTED = 6;
    PHASE_ABORTED = 7;
    PHASE_ROLLED_BACK = 8;
  }

  string migration_id = 1;
  Phase phase = 2;
  RouteTarget source = 3;
  RouteTarget target = 4;
  string base_snapshot_uri_prefix = 5;
  string final_snapshot_uri_prefix = 6;
  map<string, int64> stage_duration_ms = 7;
  google.protobuf.Timestamp start_time = 8;
  google.protobuf.Timestamp update_time = 9;
}
```

Add these request/response messages after `ResumeActorResponse`:

```proto
message PrepareActorMigrationRequest {
  ObjectRef actor = 1;
  Selector target_worker_selector = 2;
  bool require_cross_node = 3;
}

message PrepareActorMigrationResponse {
  Actor actor = 1;
}

message CommitActorMigrationRequest {
  ObjectRef actor = 1;
  int32 drain_timeout_ms = 2;
  bool require_quiesce = 3;
}

message CommitActorMigrationResponse {
  Actor actor = 1;
}

message AbortActorMigrationRequest {
  ObjectRef actor = 1;
}

message AbortActorMigrationResponse {
  Actor actor = 1;
}

message GetActorRouteRequest {
  ObjectRef actor = 1;
}

message GetActorRouteResponse {
  Actor actor = 1;
  ActorRoute route = 2;
}
```

- [ ] **Step 2: Generate protobuf code**

Run:

```bash
go generate ./pkg/proto/ateapipb
```

Expected: `pkg/proto/ateapipb/ateapi.pb.go` and `pkg/proto/ateapipb/ateapi_grpc.pb.go` contain `PrepareActorMigration`, `CommitActorMigration`, `AbortActorMigration`, and `GetActorRoute`.

- [ ] **Step 3: Verify proto formatting**

Run:

```bash
./hack/update/proto-fmt.sh
go test ./pkg/proto/ateapipb
```

Expected: proto formatting completes and the package test exits successfully.

- [ ] **Step 4: Commit**

```bash
git add pkg/proto/ateapipb/ateapi.proto pkg/proto/ateapipb/ateapi.pb.go pkg/proto/ateapipb/ateapi_grpc.pb.go
git commit -m "api: add actor hot migration route state"
```

## Task 2: Add Route Helpers in Control API

**Files:**
- Create: `cmd/ateapi/internal/controlapi/migration_route.go`
- Create: `cmd/ateapi/internal/controlapi/migration_route_test.go`

- [ ] **Step 1: Write failing tests**

Create `cmd/ateapi/internal/controlapi/migration_route_test.go`:

```go
package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestRouteTargetFromActor(t *testing.T) {
	actor := &ateapipb.Actor{
		AteomPodNamespace: "workers",
		AteomPodName:      "worker-a",
		AteomPodIp:        "10.0.0.10",
		AteomPodUid:       "uid-a",
		WorkerPoolName:    "pool-a",
	}
	got := routeTargetFromActor(actor, "node-a")
	if got.GetAteomPodIp() != "10.0.0.10" || got.GetNodeName() != "node-a" {
		t.Fatalf("route target = %+v", got)
	}
}

func TestEnsureActiveRouteInitializesFromActor(t *testing.T) {
	actor := &ateapipb.Actor{
		AteomPodNamespace: "workers",
		AteomPodName:      "worker-a",
		AteomPodIp:        "10.0.0.10",
		AteomPodUid:       "uid-a",
		WorkerPoolName:    "pool-a",
	}
	ensureActiveRoute(actor, "node-a")
	if actor.GetRoute().GetPhase() != ateapipb.ActorRoute_PHASE_ACTIVE {
		t.Fatalf("phase = %s", actor.GetRoute().GetPhase())
	}
	if actor.GetRoute().GetGeneration() != 1 {
		t.Fatalf("generation = %d", actor.GetRoute().GetGeneration())
	}
	if actor.GetRoute().GetActive().GetAteomPodName() != "worker-a" {
		t.Fatalf("active = %+v", actor.GetRoute().GetActive())
	}
}

func TestSwitchRouteToCandidate(t *testing.T) {
	actor := &ateapipb.Actor{
		Route: &ateapipb.ActorRoute{
			Phase:      ateapipb.ActorRoute_PHASE_DRAINING,
			Generation: 4,
			Active:     &ateapipb.RouteTarget{AteomPodName: "source", AteomPodIp: "10.0.0.10"},
			Candidate:  &ateapipb.RouteTarget{AteomPodName: "target", AteomPodIp: "10.0.0.11"},
		},
	}
	switchRouteToCandidate(actor)
	if actor.GetRoute().GetPhase() != ateapipb.ActorRoute_PHASE_SWITCHED {
		t.Fatalf("phase = %s", actor.GetRoute().GetPhase())
	}
	if actor.GetRoute().GetGeneration() != 5 {
		t.Fatalf("generation = %d", actor.GetRoute().GetGeneration())
	}
	if actor.GetRoute().GetActive().GetAteomPodName() != "target" {
		t.Fatalf("active = %+v", actor.GetRoute().GetActive())
	}
	if actor.GetRoute().GetCandidate() != nil {
		t.Fatalf("candidate should be cleared")
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi -run 'Test(RouteTargetFromActor|EnsureActiveRouteInitializesFromActor|SwitchRouteToCandidate)' -count=1
```

Expected: build fails because helper functions are undefined.

- [ ] **Step 3: Implement helpers**

Create `cmd/ateapi/internal/controlapi/migration_route.go`:

```go
package controlapi

import "github.com/agent-substrate/substrate/pkg/proto/ateapipb"

func routeTargetFromActor(actor *ateapipb.Actor, nodeName string) *ateapipb.RouteTarget {
	return &ateapipb.RouteTarget{
		AteomPodNamespace: actor.GetAteomPodNamespace(),
		AteomPodName:      actor.GetAteomPodName(),
		AteomPodIp:        actor.GetAteomPodIp(),
		AteomPodUid:       actor.GetAteomPodUid(),
		WorkerPoolName:    actor.GetWorkerPoolName(),
		NodeName:          nodeName,
	}
}

func ensureActiveRoute(actor *ateapipb.Actor, nodeName string) {
	if actor.GetRoute().GetActive().GetAteomPodIp() != "" {
		return
	}
	actor.Route = &ateapipb.ActorRoute{
		Active:     routeTargetFromActor(actor, nodeName),
		Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
		Generation: 1,
	}
}

func switchRouteToCandidate(actor *ateapipb.Actor) {
	route := actor.GetRoute()
	actor.Route = &ateapipb.ActorRoute{
		Active:     route.GetCandidate(),
		Phase:      ateapipb.ActorRoute_PHASE_SWITCHED,
		Generation: route.GetGeneration() + 1,
	}
	actor.AteomPodNamespace = actor.GetRoute().GetActive().GetAteomPodNamespace()
	actor.AteomPodName = actor.GetRoute().GetActive().GetAteomPodName()
	actor.AteomPodIp = actor.GetRoute().GetActive().GetAteomPodIp()
	actor.AteomPodUid = actor.GetRoute().GetActive().GetAteomPodUid()
	actor.WorkerPoolName = actor.GetRoute().GetActive().GetWorkerPoolName()
}
```

- [ ] **Step 4: Verify tests pass**

Run:

```bash
gofmt -w cmd/ateapi/internal/controlapi/migration_route.go cmd/ateapi/internal/controlapi/migration_route_test.go
go test ./cmd/ateapi/internal/controlapi -run 'Test(RouteTargetFromActor|EnsureActiveRouteInitializesFromActor|SwitchRouteToCandidate)' -count=1
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add cmd/ateapi/internal/controlapi/migration_route.go cmd/ateapi/internal/controlapi/migration_route_test.go
git commit -m "controlapi: add actor route helpers"
```

## Task 3: Implement PrepareActorMigration Workflow

**Files:**
- Create: `cmd/ateapi/internal/controlapi/workflow_migrate.go`
- Create: `cmd/ateapi/internal/controlapi/workflow_migrate_test.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow.go`
- Modify: `cmd/ateapi/internal/controlapi/controlapi.go` or the file that registers existing RPC handlers

- [ ] **Step 1: Write transition tests**

Create tests in `cmd/ateapi/internal/controlapi/workflow_migrate_test.go` covering:

```go
func TestPrepareMigrationRejectsNonRunningActor(t *testing.T) {
	actor := &ateapipb.Actor{Status: ateapipb.Actor_STATUS_SUSPENDED}
	err := validatePrepareMigrationActor(actor)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepareMigrationRequiresCrossNodeWhenRequested(t *testing.T) {
	source := &ateapipb.RouteTarget{NodeName: "node-a"}
	target := &ateapipb.RouteTarget{NodeName: "node-a"}
	err := validateMigrationTarget(source, target, true)
	if err == nil {
		t.Fatal("expected cross-node validation error")
	}
}

func TestPrepareMigrationSetsPreparingRoute(t *testing.T) {
	actor := &ateapipb.Actor{
		Status: ateapipb.Actor_STATUS_RUNNING,
		Route: &ateapipb.ActorRoute{
			Active:     &ateapipb.RouteTarget{AteomPodName: "source", AteomPodIp: "10.0.0.10", NodeName: "node-a"},
			Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
			Generation: 1,
		},
	}
	target := &ateapipb.RouteTarget{AteomPodName: "target", AteomPodIp: "10.0.0.11", NodeName: "node-b"}
	markMigrationPreparing(actor, "migration-1", target, "gs://bucket/base")
	if actor.GetRoute().GetPhase() != ateapipb.ActorRoute_PHASE_PREPARING {
		t.Fatalf("route phase = %s", actor.GetRoute().GetPhase())
	}
	if actor.GetRoute().GetCandidate().GetAteomPodName() != "target" {
		t.Fatalf("candidate = %+v", actor.GetRoute().GetCandidate())
	}
	if actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_PREPARING {
		t.Fatalf("migration phase = %s", actor.GetMigration().GetPhase())
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi -run 'TestPrepareMigration' -count=1
```

Expected: build fails for undefined migration helpers.

- [ ] **Step 3: Implement prepare helpers and workflow shape**

Create `cmd/ateapi/internal/controlapi/workflow_migrate.go` with:

```go
package controlapi

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type MigrateInput struct {
	ActorName        string
	Atespace         string
	RequireCrossNode bool
	DrainTimeout     time.Duration
	RequireQuiesce   bool
}

type MigrateState struct {
	Actor *ateapipb.Actor
}

func validatePrepareMigrationActor(actor *ateapipb.Actor) error {
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return status.Errorf(codes.FailedPrecondition, "actor must be RUNNING for hot migration, got %s", actor.GetStatus())
	}
	if actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_UNSPECIFIED &&
		actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_COMMITTED &&
		actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_ABORTED &&
		actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_ROLLED_BACK {
		return status.Errorf(codes.Aborted, "actor migration already in progress: %s", actor.GetMigration().GetPhase())
	}
	return nil
}

func validateMigrationTarget(source, target *ateapipb.RouteTarget, requireCrossNode bool) error {
	if target.GetAteomPodIp() == "" {
		return status.Error(codes.FailedPrecondition, "candidate target has no pod IP")
	}
	if requireCrossNode && source.GetNodeName() == target.GetNodeName() {
		return status.Errorf(codes.FailedPrecondition, "candidate target is on source node %q", source.GetNodeName())
	}
	return nil
}

func markMigrationPreparing(actor *ateapipb.Actor, migrationID string, target *ateapipb.RouteTarget, baseSnapshot string) {
	now := timestamppb.Now()
	source := actor.GetRoute().GetActive()
	actor.Route = &ateapipb.ActorRoute{
		Active:     source,
		Candidate:  target,
		Phase:      ateapipb.ActorRoute_PHASE_PREPARING,
		Generation: actor.GetRoute().GetGeneration(),
	}
	actor.Migration = &ateapipb.ActorMigration{
		MigrationId:           migrationID,
		Phase:                 ateapipb.ActorMigration_PHASE_PREPARING,
		Source:                source,
		Target:                target,
		BaseSnapshotUriPrefix: baseSnapshot,
		StageDurationMs:       map[string]int64{},
		StartTime:             now,
		UpdateTime:            now,
	}
}

func (w *ActorWorkflow) PrepareActorMigration(ctx context.Context, atespace, name string, requireCrossNode bool) (*ateapipb.Actor, error) {
	return nil, fmt.Errorf("PrepareActorMigration workflow wiring is implemented in this task after worker selection extraction")
}
```

Then replace the temporary return in `PrepareActorMigration` with a workflow that:

- acquires the actor lock with `w.acquireActorLock`;
- loads the actor and template;
- ensures an active route from the current worker;
- chooses a free target worker that is not the source worker and, when requested, not on the source node;
- marks the actor route as `PHASE_PREPARING`;
- calls atelet restore on the candidate worker using the latest external snapshot;
- marks migration as `PHASE_TARGET_READY` while keeping route phase `PHASE_PREPARING`.

- [ ] **Step 4: Register the RPC handler**

In the existing control API server file that handles `ResumeActor`, add:

```go
func (s *Service) PrepareActorMigration(ctx context.Context, req *ateapipb.PrepareActorMigrationRequest) (*ateapipb.PrepareActorMigrationResponse, error) {
	ref := req.GetActor()
	actor, err := s.workflow.PrepareActorMigration(ctx, ref.GetAtespace(), ref.GetName(), req.GetRequireCrossNode())
	if err != nil {
		return nil, err
	}
	return &ateapipb.PrepareActorMigrationResponse{Actor: actor}, nil
}
```

Use the actual server receiver name already used by `ResumeActor`.

- [ ] **Step 5: Verify prepare workflow tests**

Run:

```bash
gofmt -w cmd/ateapi/internal/controlapi/workflow_migrate.go cmd/ateapi/internal/controlapi/workflow_migrate_test.go
go test ./cmd/ateapi/internal/controlapi -run 'TestPrepareMigration' -count=1
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/ateapi/internal/controlapi/workflow_migrate.go cmd/ateapi/internal/controlapi/workflow_migrate_test.go cmd/ateapi/internal/controlapi
git commit -m "controlapi: prepare actor hot migration target"
```

## Task 4: Implement Commit and Abort Migration

**Files:**
- Modify: `cmd/ateapi/internal/controlapi/workflow_migrate.go`
- Modify: `cmd/ateapi/internal/controlapi/workflow_migrate_test.go`
- Modify: RPC handler file from Task 3

- [ ] **Step 1: Add commit/abort tests**

Append tests:

```go
func TestCommitMigrationSwitchesRoute(t *testing.T) {
	actor := &ateapipb.Actor{
		Route: &ateapipb.ActorRoute{
			Active:     &ateapipb.RouteTarget{AteomPodName: "source", AteomPodIp: "10.0.0.10"},
			Candidate:  &ateapipb.RouteTarget{AteomPodName: "target", AteomPodIp: "10.0.0.11"},
			Phase:      ateapipb.ActorRoute_PHASE_DRAINING,
			Generation: 2,
		},
		Migration: &ateapipb.ActorMigration{Phase: ateapipb.ActorMigration_PHASE_FINALIZING},
	}
	markMigrationSwitched(actor, "gs://bucket/final")
	if actor.GetRoute().GetActive().GetAteomPodName() != "target" {
		t.Fatalf("active = %+v", actor.GetRoute().GetActive())
	}
	if actor.GetRoute().GetGeneration() != 3 {
		t.Fatalf("generation = %d", actor.GetRoute().GetGeneration())
	}
	if actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_SWITCHED {
		t.Fatalf("migration phase = %s", actor.GetMigration().GetPhase())
	}
}

func TestAbortMigrationRestoresActiveRoute(t *testing.T) {
	actor := &ateapipb.Actor{
		Route: &ateapipb.ActorRoute{
			Active:     &ateapipb.RouteTarget{AteomPodName: "source", AteomPodIp: "10.0.0.10"},
			Candidate:  &ateapipb.RouteTarget{AteomPodName: "target", AteomPodIp: "10.0.0.11"},
			Phase:      ateapipb.ActorRoute_PHASE_PREPARING,
			Generation: 2,
		},
		Migration: &ateapipb.ActorMigration{Phase: ateapipb.ActorMigration_PHASE_TARGET_READY},
	}
	markMigrationAborted(actor)
	if actor.GetRoute().GetPhase() != ateapipb.ActorRoute_PHASE_ACTIVE {
		t.Fatalf("route phase = %s", actor.GetRoute().GetPhase())
	}
	if actor.GetRoute().GetCandidate() != nil {
		t.Fatalf("candidate should be cleared")
	}
	if actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_ABORTED {
		t.Fatalf("migration phase = %s", actor.GetMigration().GetPhase())
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi -run 'Test(CommitMigration|AbortMigration)' -count=1
```

Expected: build fails for undefined helpers.

- [ ] **Step 3: Implement commit/abort helpers**

Add to `workflow_migrate.go`:

```go
func markMigrationDraining(actor *ateapipb.Actor, drainTimeout time.Duration) {
	actor.Route.Phase = ateapipb.ActorRoute_PHASE_DRAINING
	actor.Route.DrainDeadline = timestamppb.New(time.Now().Add(drainTimeout))
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_DRAINING
	actor.Migration.UpdateTime = timestamppb.Now()
}

func markMigrationFinalizing(actor *ateapipb.Actor) {
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_FINALIZING
	actor.Migration.UpdateTime = timestamppb.Now()
}

func markMigrationSwitched(actor *ateapipb.Actor, finalSnapshot string) {
	switchRouteToCandidate(actor)
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_SWITCHED
	actor.Migration.FinalSnapshotUriPrefix = finalSnapshot
	actor.Migration.UpdateTime = timestamppb.Now()
}

func markMigrationCommitted(actor *ateapipb.Actor) {
	actor.Route.Phase = ateapipb.ActorRoute_PHASE_ACTIVE
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_COMMITTED
	actor.Migration.UpdateTime = timestamppb.Now()
}

func markMigrationAborted(actor *ateapipb.Actor) {
	actor.Route = &ateapipb.ActorRoute{
		Active:     actor.GetRoute().GetActive(),
		Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
		Generation: actor.GetRoute().GetGeneration(),
	}
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_ABORTED
	actor.Migration.UpdateTime = timestamppb.Now()
}
```

- [ ] **Step 4: Implement `CommitActorMigration` workflow**

Add method:

```go
func (w *ActorWorkflow) CommitActorMigration(ctx context.Context, atespace, name string, drainTimeout time.Duration, requireQuiesce bool) (*ateapipb.Actor, error) {
	ctx, releaseLock, err := w.acquireActorLock(ctx, atespace, name, 45*time.Second, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	actor, err := w.store.GetActor(ctx, atespace, name)
	if err != nil {
		return nil, err
	}
	if actor.GetMigration().GetPhase() != ateapipb.ActorMigration_PHASE_TARGET_READY {
		return nil, status.Errorf(codes.FailedPrecondition, "migration target is not ready: %s", actor.GetMigration().GetPhase())
	}

	markMigrationDraining(actor, drainTimeout)
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}

	markMigrationFinalizing(actor)
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}

	finalSnapshot := actor.GetMigration().GetBaseSnapshotUriPrefix()
	markMigrationSwitched(actor, finalSnapshot)
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}

	markMigrationCommitted(actor)
	return w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
}
```

Then replace `finalSnapshot := baseSnapshot` with the real final checkpoint call:

- dial source atelet from `actor.GetMigration().GetSource()`;
- call checkpoint to a new snapshot URI;
- restore that final snapshot on the target atelet;
- record `final_checkpoint_ms` and `target_final_restore_ms`.

- [ ] **Step 5: Implement `AbortActorMigration` workflow**

Add method:

```go
func (w *ActorWorkflow) AbortActorMigration(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	ctx, releaseLock, err := w.acquireActorLock(ctx, atespace, name, 30*time.Second, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	actor, err := w.store.GetActor(ctx, atespace, name)
	if err != nil {
		return nil, err
	}
	if actor.GetRoute().GetCandidate() == nil {
		return actor, nil
	}
	markMigrationAborted(actor)
	return w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
}
```

Release the candidate worker after the actor update, checking assignment ownership before clearing it.

- [ ] **Step 6: Register commit/abort RPCs**

Add handlers:

```go
func (s *Service) CommitActorMigration(ctx context.Context, req *ateapipb.CommitActorMigrationRequest) (*ateapipb.CommitActorMigrationResponse, error) {
	ref := req.GetActor()
	drain := time.Duration(req.GetDrainTimeoutMs()) * time.Millisecond
	if drain <= 0 {
		drain = 3 * time.Second
	}
	actor, err := s.workflow.CommitActorMigration(ctx, ref.GetAtespace(), ref.GetName(), drain, req.GetRequireQuiesce())
	if err != nil {
		return nil, err
	}
	return &ateapipb.CommitActorMigrationResponse{Actor: actor}, nil
}

func (s *Service) AbortActorMigration(ctx context.Context, req *ateapipb.AbortActorMigrationRequest) (*ateapipb.AbortActorMigrationResponse, error) {
	ref := req.GetActor()
	actor, err := s.workflow.AbortActorMigration(ctx, ref.GetAtespace(), ref.GetName())
	if err != nil {
		return nil, err
	}
	return &ateapipb.AbortActorMigrationResponse{Actor: actor}, nil
}
```

- [ ] **Step 7: Verify control API migration tests**

Run:

```bash
gofmt -w cmd/ateapi/internal/controlapi/workflow_migrate.go cmd/ateapi/internal/controlapi/workflow_migrate_test.go
go test ./cmd/ateapi/internal/controlapi -run 'Test(PrepareMigration|CommitMigration|AbortMigration)' -count=1
```

Expected: tests pass.

- [ ] **Step 8: Commit**

```bash
git add cmd/ateapi/internal/controlapi
git commit -m "controlapi: commit and abort actor hot migration"
```

## Task 5: Add Router Route Resolver

**Files:**
- Create: `cmd/atenet/internal/router/route_resolver.go`
- Create: `cmd/atenet/internal/router/route_resolver_test.go`
- Modify: `cmd/atenet/internal/router/extproc.go`
- Modify: `cmd/atenet/internal/router/direct_proxy.go`

- [ ] **Step 1: Write route resolver tests**

Create `cmd/atenet/internal/router/route_resolver_test.go`:

```go
package router

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestTargetForRouteActive(t *testing.T) {
	actor := &ateapipb.Actor{Route: &ateapipb.ActorRoute{
		Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.10"},
		Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
		Generation: 3,
	}}
	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}
	if got.IP != "10.0.0.10" || got.Generation != 3 {
		t.Fatalf("target = %+v", got)
	}
}

func TestTargetForRoutePreparingStaysOnSource(t *testing.T) {
	actor := &ateapipb.Actor{Route: &ateapipb.ActorRoute{
		Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.10"},
		Candidate:  &ateapipb.RouteTarget{AteomPodIp: "10.0.0.11"},
		Phase:      ateapipb.ActorRoute_PHASE_PREPARING,
		Generation: 3,
	}}
	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}
	if got.IP != "10.0.0.10" {
		t.Fatalf("target = %+v", got)
	}
}

func TestTargetForRouteSwitchedUsesTarget(t *testing.T) {
	actor := &ateapipb.Actor{Route: &ateapipb.ActorRoute{
		Active:     &ateapipb.RouteTarget{AteomPodIp: "10.0.0.11"},
		Phase:      ateapipb.ActorRoute_PHASE_SWITCHED,
		Generation: 4,
	}}
	got, err := targetForActorRoute(actor)
	if err != nil {
		t.Fatal(err)
	}
	if got.IP != "10.0.0.11" || got.Generation != 4 {
		t.Fatalf("target = %+v", got)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./cmd/atenet/internal/router -run 'TestTargetForRoute' -count=1
```

Expected: build fails for undefined resolver types.

- [ ] **Step 3: Implement route resolver**

Create `cmd/atenet/internal/router/route_resolver.go`:

```go
package router

import (
	"context"
	"fmt"
	"net"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

type routeTarget struct {
	IP         string
	Port       string
	Generation int64
	Phase      ateapipb.ActorRoute_Phase
}

type ActorRouteResolver struct {
	apiClient ateapipb.ControlClient
	resumer   *ActorResumer
}

func NewActorRouteResolver(apiClient ateapipb.ControlClient, resumer *ActorResumer) *ActorRouteResolver {
	return &ActorRouteResolver{apiClient: apiClient, resumer: resumer}
}

func (r *ActorRouteResolver) Resolve(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, routeTarget, error) {
	actor, err := r.getActorOrResume(ctx, atespace, actorName)
	if err != nil {
		return nil, routeTarget{}, err
	}
	target, err := targetForActorRoute(actor)
	if err != nil {
		return actor, routeTarget{}, err
	}
	return actor, target, nil
}

func (r *ActorRouteResolver) getActorOrResume(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error) {
	resp, err := r.apiClient.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName}})
	if err == nil && resp.GetStatus() == ateapipb.Actor_STATUS_RUNNING {
		return resp, nil
	}
	return r.resumer.ResumeActor(ctx, atespace, actorName)
}

func targetForActorRoute(actor *ateapipb.Actor) (routeTarget, error) {
	route := actor.GetRoute()
	target := route.GetActive()
	if target.GetAteomPodIp() == "" {
		target = &ateapipb.RouteTarget{AteomPodIp: actor.GetAteomPodIp()}
	}
	ip := target.GetAteomPodIp()
	if net.ParseIP(ip) == nil {
		return routeTarget{}, newReqError(envoy_type.StatusCode_InternalServerError, "actor %q route has invalid target IP %q", actor.GetMetadata().GetName(), ip)
	}
	return routeTarget{
		IP:         ip,
		Port:       "80",
		Generation: route.GetGeneration(),
		Phase:      route.GetPhase(),
	}, nil
}

func (t routeTarget) HostPort() string {
	return net.JoinHostPort(t.IP, t.Port)
}

func (t routeTarget) String() string {
	return fmt.Sprintf("%s generation=%d phase=%s", t.HostPort(), t.Generation, t.Phase)
}
```

- [ ] **Step 4: Wire resolver into router server**

Add a field to `ExtProcServer`:

```go
routeResolver *ActorRouteResolver
```

Initialize it in `NewExtProcServer`:

```go
resumer := NewActorResumer(apiClient)
return &ExtProcServer{
	port:          port,
	apiClient:     apiClient,
	recorder:      NewQueryRecorder(100),
	resumer:       resumer,
	routeResolver: NewActorRouteResolver(apiClient, resumer),
	routeDuration: routeDuration,
}
```

Replace direct `s.resumer.ResumeActor` calls in `extproc.go` and `direct_proxy.go` with:

```go
actor, target, err := s.extprocSrv.routeResolver.Resolve(req.Context(), atespace, actorName)
```

For `extproc.go`, use `s.routeResolver.Resolve(ctx, atespace, actorName)`.

- [ ] **Step 5: Verify router tests**

Run:

```bash
gofmt -w cmd/atenet/internal/router/route_resolver.go cmd/atenet/internal/router/route_resolver_test.go cmd/atenet/internal/router/extproc.go cmd/atenet/internal/router/direct_proxy.go
go test ./cmd/atenet/internal/router -run 'TestTargetForRoute|TestExtProc|TestDirect' -count=1
```

Expected: route resolver tests pass and existing router tests still pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/atenet/internal/router
git commit -m "router: resolve actor routes during migration"
```

## Task 6: Add Router In-Flight Drain

**Files:**
- Create: `cmd/atenet/internal/router/inflight.go`
- Create: `cmd/atenet/internal/router/inflight_test.go`
- Modify: `cmd/atenet/internal/router/direct_proxy.go`

- [ ] **Step 1: Write drain tests**

Create `cmd/atenet/internal/router/inflight_test.go`:

```go
package router

import (
	"context"
	"testing"
	"time"
)

func TestInFlightTrackerWaitsForDone(t *testing.T) {
	tracker := newInFlightTracker()
	done := tracker.begin("space/actor")
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- tracker.waitZero(context.Background(), "space/actor")
	}()
	time.Sleep(20 * time.Millisecond)
	done()
	select {
	case err := <-waitCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitZero timed out")
	}
}

func TestInFlightTrackerHonorsContext(t *testing.T) {
	tracker := newInFlightTracker()
	_ = tracker.begin("space/actor")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := tracker.waitZero(ctx, "space/actor"); err == nil {
		t.Fatal("expected context timeout")
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./cmd/atenet/internal/router -run 'TestInFlightTracker' -count=1
```

Expected: build fails for undefined tracker.

- [ ] **Step 3: Implement tracker**

Create `cmd/atenet/internal/router/inflight.go`:

```go
package router

import (
	"context"
	"sync"
)

type inFlightTracker struct {
	mu     sync.Mutex
	counts map[string]int
	waitCh map[string]chan struct{}
}

func newInFlightTracker() *inFlightTracker {
	return &inFlightTracker{
		counts: map[string]int{},
		waitCh: map[string]chan struct{}{},
	}
}

func (t *inFlightTracker) begin(key string) func() {
	t.mu.Lock()
	t.counts[key]++
	if t.waitCh[key] == nil {
		t.waitCh[key] = make(chan struct{})
	}
	t.mu.Unlock()
	return func() { t.end(key) }
}

func (t *inFlightTracker) end(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts[key] > 0 {
		t.counts[key]--
	}
	if t.counts[key] == 0 {
		close(t.waitCh[key])
		delete(t.waitCh, key)
	}
}

func (t *inFlightTracker) waitZero(ctx context.Context, key string) error {
	t.mu.Lock()
	if t.counts[key] == 0 {
		t.mu.Unlock()
		return nil
	}
	ch := t.waitCh[key]
	t.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}
```

- [ ] **Step 4: Wire into direct proxy**

Add to `RouterServer`:

```go
inflight *inFlightTracker
```

Initialize in `NewRouterServer`:

```go
inflight: newInFlightTracker(),
```

In `handleDirectProxy`, after parsing actor ref:

```go
actorKey := atespace + "/" + actorName
done := s.inflight.begin(actorKey)
defer done()
```

When route phase is `PHASE_DRAINING`, wait until the route switches or request deadline expires by re-resolving route after a short sleep. Keep this simple in version 1:

```go
if target.Phase == ateapipb.ActorRoute_PHASE_DRAINING {
	time.Sleep(50 * time.Millisecond)
	actor, target, err = s.extprocSrv.routeResolver.Resolve(req.Context(), atespace, actorName)
	if err != nil {
		writeReqError(w, mapResumeError(actorName, err))
		return
	}
}
```

The control plane owns the actual drain deadline; the router protects request accounting and avoids routing new direct-proxy requests to an obviously draining source.

- [ ] **Step 5: Verify router drain tests**

Run:

```bash
gofmt -w cmd/atenet/internal/router/inflight.go cmd/atenet/internal/router/inflight_test.go cmd/atenet/internal/router/direct_proxy.go cmd/atenet/internal/router/router.go
go test ./cmd/atenet/internal/router -run 'TestInFlightTracker|TestTargetForRoute' -count=1
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/atenet/internal/router
git commit -m "router: track in-flight actor requests"
```

## Task 7: Add Demo Workload with Quiesce and Verification Endpoints

**Files:**
- Create: `demos/memory/hot_migration_server.go`
- Create: `demos/memory/hot_migration_server_test.go`
- Modify: demo image build config if demos are listed explicitly

- [ ] **Step 1: Write demo tests**

Create `demos/memory/hot_migration_server_test.go`:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCounterStopsMutatingWhenQuiesced(t *testing.T) {
	s := newHotMigrationServer(64 << 20)
	req := httptest.NewRequest(http.MethodPost, "/substrate/quiesce", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("quiesce status = %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/increment", nil)
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("increment while quiesced status = %d", rec.Code)
	}
}

func TestMigrationStateIncludesChecksum(t *testing.T) {
	s := newHotMigrationServer(64 << 20)
	req := httptest.NewRequest(http.MethodGet, "/substrate/migration-state", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state status = %d", rec.Code)
	}
	if body := rec.Body.String(); body == "" || body == "{}\n" {
		t.Fatalf("unexpected body %q", body)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./demos/memory -run 'Test(CounterStopsMutatingWhenQuiesced|MigrationStateIncludesChecksum)' -count=1
```

Expected: build fails because demo server is not implemented.

- [ ] **Step 3: Implement demo workload**

Create `demos/memory/hot_migration_server.go`:

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net/http"
	"sync"
	"sync/atomic"
)

type hotMigrationServer struct {
	mux      *http.ServeMux
	payload  []byte
	counter  atomic.Int64
	quiesced atomic.Bool
	mu       sync.Mutex
}

func newHotMigrationServer(bytes int64) *hotMigrationServer {
	s := &hotMigrationServer{mux: http.NewServeMux(), payload: make([]byte, bytes)}
	for i := range s.payload {
		s.payload[i] = byte(i)
	}
	s.mux.HandleFunc("/readyz", s.readyz)
	s.mux.HandleFunc("/increment", s.increment)
	s.mux.HandleFunc("/substrate/quiesce", s.quiesce)
	s.mux.HandleFunc("/substrate/resume", s.resume)
	s.mux.HandleFunc("/substrate/migration-state", s.state)
	return s
}

func (s *hotMigrationServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *hotMigrationServer) readyz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *hotMigrationServer) increment(w http.ResponseWriter, r *http.Request) {
	if s.quiesced.Load() {
		http.Error(w, "quiesced", http.StatusServiceUnavailable)
		return
	}
	v := s.counter.Add(1)
	_ = json.NewEncoder(w).Encode(map[string]int64{"counter": v})
}

func (s *hotMigrationServer) quiesce(w http.ResponseWriter, r *http.Request) {
	s.quiesced.Store(true)
	w.WriteHeader(http.StatusOK)
}

func (s *hotMigrationServer) resume(w http.ResponseWriter, r *http.Request) {
	s.quiesced.Store(false)
	w.WriteHeader(http.StatusOK)
}

func (s *hotMigrationServer) state(w http.ResponseWriter, r *http.Request) {
	sum := sha256.Sum256(s.payload)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"counter":  s.counter.Load(),
		"checksum": hex.EncodeToString(sum[:]),
		"quiesced": s.quiesced.Load(),
	})
}

func main() {
	var bytes int64
	flag.Int64Var(&bytes, "bytes", 16<<30, "bytes to allocate")
	flag.Parse()
	http.ListenAndServe(":80", newHotMigrationServer(bytes))
}
```

- [ ] **Step 4: Verify demo tests**

Run:

```bash
gofmt -w demos/memory/hot_migration_server.go demos/memory/hot_migration_server_test.go
go test ./demos/memory -count=1
```

Expected: tests pass.

- [ ] **Step 5: Commit**

```bash
git add demos/memory
git commit -m "demo: add hot migration verification workload"
```

## Task 8: Add Perfkit Hot Migration Runner

**Files:**
- Create: `benchmarking/perfkit/internal/runner/hot_migration.go`
- Create: `benchmarking/perfkit/internal/runner/hot_migration_test.go`
- Modify: `benchmarking/perfkit/main.go`

- [ ] **Step 1: Write result calculation tests**

Create `benchmarking/perfkit/internal/runner/hot_migration_test.go`:

```go
package runner

import (
	"testing"
	"time"
)

func TestHotMigrationSummaryComputesBrownout(t *testing.T) {
	events := []hotMigrationEvent{
		{Name: "http_ok", At: time.Unix(10, 0)},
		{Name: "migration_start", At: time.Unix(11, 0)},
		{Name: "http_ok", At: time.Unix(14, 0), Generation: 2},
	}
	got := summarizeHotMigration(events)
	if got.LongestHTTPGapMS != 4000 {
		t.Fatalf("LongestHTTPGapMS = %d", got.LongestHTTPGapMS)
	}
	if got.StartToTargetHTTPMS != 3000 {
		t.Fatalf("StartToTargetHTTPMS = %d", got.StartToTargetHTTPMS)
	}
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
go test ./benchmarking/perfkit/internal/runner -run TestHotMigrationSummaryComputesBrownout -count=1
```

Expected: build fails because result types are undefined.

- [ ] **Step 3: Implement summary types**

Create `benchmarking/perfkit/internal/runner/hot_migration.go`:

```go
package runner

import "time"

type hotMigrationEvent struct {
	Name       string    `json:"name"`
	At         time.Time `json:"at"`
	StatusCode int       `json:"status_code,omitempty"`
	Generation int64     `json:"generation,omitempty"`
	Node       string    `json:"node,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type hotMigrationSummary struct {
	LongestHTTPGapMS    int64 `json:"longest_http_gap_ms"`
	StartToTargetHTTPMS int64 `json:"start_to_target_http_ms"`
	HTTP5xx            int   `json:"http_5xx"`
}

func summarizeHotMigration(events []hotMigrationEvent) hotMigrationSummary {
	var out hotMigrationSummary
	var lastOK time.Time
	var start time.Time
	for _, ev := range events {
		if ev.Name == "migration_start" {
			start = ev.At
		}
		if ev.Name == "http_ok" {
			if !lastOK.IsZero() {
				gap := ev.At.Sub(lastOK).Milliseconds()
				if gap > out.LongestHTTPGapMS {
					out.LongestHTTPGapMS = gap
				}
			}
			lastOK = ev.At
			if !start.IsZero() && ev.Generation > 1 && out.StartToTargetHTTPMS == 0 {
				out.StartToTargetHTTPMS = ev.At.Sub(start).Milliseconds()
			}
		}
		if ev.StatusCode >= 500 {
			out.HTTP5xx++
		}
	}
	return out
}
```

- [ ] **Step 4: Implement e2e command path**

Add a perfkit mode that:

1. Creates a 16GiB memory actor.
2. Starts a goroutine issuing HTTP requests through `atenet-router` every 100ms.
3. Records `http_ok`, status code, response counter, checksum, route generation, and serving node.
4. Calls `PrepareActorMigration` with `require_cross_node=true`.
5. Calls `CommitActorMigration` with `drain_timeout_ms=3000` and `require_quiesce=true`.
6. Continues HTTP probes for 10s after commit.
7. Writes raw JSONL and summary JSON under `results/hot-migration-<timestamp>/`.

The result JSON must include:

```json
{
  "memory_bytes": 17179869184,
  "source_node": "node-a",
  "target_node": "node-b",
  "longest_http_gap_ms": 0,
  "start_to_target_http_ms": 0,
  "http_5xx": 0,
  "source_generation": 1,
  "target_generation": 2,
  "checksum_before": "",
  "checksum_after": "",
  "counter_before": 0,
  "counter_after": 0
}
```

- [ ] **Step 5: Verify perfkit tests**

Run:

```bash
gofmt -w benchmarking/perfkit/internal/runner/hot_migration.go benchmarking/perfkit/internal/runner/hot_migration_test.go benchmarking/perfkit/main.go
go test ./benchmarking/perfkit/internal/runner ./benchmarking/perfkit -run 'TestHotMigration|TestMain' -count=1
```

Expected: tests pass.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/perfkit
git commit -m "perfkit: add hot migration benchmark"
```

## Task 9: End-to-End Validation on g8ine

**Files:**
- Create: `docs/hot-migration-runbook.md`
- Writes runtime data under: `results/hot-migration-*/`

- [ ] **Step 1: Write runbook**

Create `docs/hot-migration-runbook.md`:

```markdown
# Hot Migration Runbook

## Target

Validate 16GiB actor hot migration across two different nodes through the stable atenet router endpoint.

## Required cluster shape

- Instance family: `g8ine.4xlarge` first, `g8ine.8xlarge` only if 4x cannot saturate the path.
- At least two worker nodes.
- Router direct HTTP proxy enabled.
- Snapshot backend with enough capacity for at least two 16GiB snapshots.

## Command

```bash
KUBECONFIG=/tmp/p2p-ng.kubeconfig go run ./benchmarking/perfkit \
  --scenario=hot-migration \
  --memory-bytes=17179869184 \
  --require-cross-node=true \
  --drain-timeout=3s \
  --http-probe-interval=100ms \
  --output-dir=results/hot-migration-$(date -u +%Y%m%dT%H%M%SZ)
```

## Pass criteria

- source_node != target_node
- http_5xx == 0
- checksum_before == checksum_after
- counter_after >= counter_before
- start_to_target_http_ms <= 30000
- longest_http_gap_ms <= 3000
```

- [ ] **Step 2: Build and deploy**

Run:

```bash
make build-images
KUBECONFIG=/tmp/p2p-ng.kubeconfig ./hack/install-ate.sh
```

Expected: all ateapi, atelet, ateom, and atenet pods roll to the new images.

- [ ] **Step 3: Run 16GiB hot migration benchmark**

Run the command from the runbook.

Expected: a result directory containing raw JSONL and summary JSON.

- [ ] **Step 4: Extract the core numbers**

Run:

```bash
jq '{source_node,target_node,longest_http_gap_ms,start_to_target_http_ms,http_5xx,source_generation,target_generation,checksum_before,checksum_after,counter_before,counter_after}' results/hot-migration-*/summary.json
```

Expected:

```json
{
  "source_node": "node-a",
  "target_node": "node-b",
  "longest_http_gap_ms": 3000,
  "start_to_target_http_ms": 30000,
  "http_5xx": 0,
  "source_generation": 1,
  "target_generation": 2,
  "checksum_before": "same",
  "checksum_after": "same",
  "counter_before": 1,
  "counter_after": 1
}
```

The actual values replace the sample values. If `start_to_target_http_ms > 30000`, inspect `final_checkpoint_ms`; if it is close to the earlier 12s baseline and brownout is still low, the system meets request-level hot migration but not aggressive total-time SLO. If brownout is high, continue to Task 10.

- [ ] **Step 5: Commit runbook**

```bash
git add docs/hot-migration-runbook.md
git commit -m "docs: add hot migration runbook"
```

## Task 10: Optimize Finalization if the 30s SLO Fails

**Files:**
- Modify: `cmd/ateapi/internal/controlapi/workflow_migrate.go`
- Modify: `cmd/atelet/main.go`
- Modify: `internal/proto/ateletpb/atelet.proto`
- Modify: generated `internal/proto/ateletpb/atelet.pb.go`
- Modify: generated `internal/proto/ateletpb/atelet_grpc.pb.go`

- [ ] **Step 1: Decide from data**

Use the Task 9 summary:

```bash
jq '.stage_duration_ms' results/hot-migration-*/summary.json
```

If `final_checkpoint_ms <= 3000` and total is still over 30s, optimize prepare scheduling or target restore. If `final_checkpoint_ms > 3000`, implement final delta or compact app-state checkpoint.

- [ ] **Step 2: Add checkpoint mode for finalization**

Extend `internal/proto/ateletpb/atelet.proto` checkpoint request with:

```proto
  bool prefer_incremental = 20;
```

Run:

```bash
go generate ./internal/proto/ateletpb
```

- [ ] **Step 3: Use incremental final checkpoint in migration**

In `workflow_migrate.go`, set:

```go
PreferIncremental: true,
```

for the final migration checkpoint request.

- [ ] **Step 4: Verify unit tests and rerun e2e**

Run:

```bash
go test ./cmd/ateapi/internal/controlapi ./cmd/atelet ./cmd/atenet/internal/router ./benchmarking/perfkit/internal/runner -count=1
KUBECONFIG=/tmp/p2p-ng.kubeconfig go run ./benchmarking/perfkit --scenario=hot-migration --memory-bytes=17179869184 --require-cross-node=true --drain-timeout=3s --http-probe-interval=100ms --output-dir=results/hot-migration-$(date -u +%Y%m%dT%H%M%SZ)
```

Expected: `final_checkpoint_ms` drops relative to the previous run. If it does not drop, the runtime is still scanning the full image and the next step is dirty-page tracking inside the sandbox-specific checkpoint implementation.

- [ ] **Step 5: Commit**

```bash
git add internal/proto/ateletpb/atelet.proto internal/proto/ateletpb/atelet.pb.go internal/proto/ateletpb/atelet_grpc.pb.go cmd/ateapi/internal/controlapi cmd/atelet
git commit -m "checkpoint: prefer incremental final migration snapshots"
```

## Task 11: Final Verification Matrix

**Files:**
- Writes runtime data under: `results/hot-migration-*`
- Modify: `docs/hot-migration-runbook.md`

- [ ] **Step 1: Run no-quiesce compatibility test**

Run:

```bash
KUBECONFIG=/tmp/p2p-ng.kubeconfig go run ./benchmarking/perfkit \
  --scenario=hot-migration \
  --memory-bytes=17179869184 \
  --require-cross-node=true \
  --drain-timeout=3s \
  --require-quiesce=false \
  --http-probe-interval=100ms \
  --output-dir=results/hot-migration-no-quiesce-$(date -u +%Y%m%dT%H%M%SZ)
```

Expected: HTTP availability is measured separately from write correctness and the summary labels `require_quiesce=false`.

- [ ] **Step 2: Run rollback test**

Inject target failure after prepare and before commit, then run:

```bash
KUBECONFIG=/tmp/p2p-ng.kubeconfig go run ./benchmarking/perfkit \
  --scenario=hot-migration-rollback \
  --memory-bytes=17179869184 \
  --require-cross-node=true \
  --output-dir=results/hot-migration-rollback-$(date -u +%Y%m%dT%H%M%SZ)
```

Expected: route generation does not switch to the target, HTTP remains served by source, and target worker is released.

- [ ] **Step 3: Run repeated migration stability test**

Run:

```bash
for i in 1 2 3 4 5; do
  KUBECONFIG=/tmp/p2p-ng.kubeconfig go run ./benchmarking/perfkit \
    --scenario=hot-migration \
    --memory-bytes=17179869184 \
    --require-cross-node=true \
    --drain-timeout=3s \
    --http-probe-interval=100ms \
    --output-dir=results/hot-migration-repeat-${i}-$(date -u +%Y%m%dT%H%M%SZ)
done
```

Expected: all 5 runs satisfy pass criteria or each failure has a stage-level reason.

- [ ] **Step 4: Update runbook with measured data**

Append a table:

```markdown
## Measured Results

| run_id | memory | source_node | target_node | start_to_target_http_ms | longest_http_gap_ms | http_5xx | final_checkpoint_ms | target_final_restore_ms | result |
|---|---:|---|---|---:|---:|---:|---:|---:|---|
| hot-migration-... | 16GiB | ... | ... | ... | ... | ... | ... | ... | pass |
```

- [ ] **Step 5: Commit verification data references**

```bash
git add docs/hot-migration-runbook.md
git commit -m "docs: record hot migration validation results"
```

## Self-Review

- Spec coverage: route state, make-before-break migration, router drain, quiesce, rollback, observability, and 16GiB cross-node validation each have a task.
- Confidence coverage: Task 0 front-loads proto generation, receiver naming, fake atelet feasibility, direct proxy availability, and the dual-worker functional-test requirement.
- Placeholder scan: the plan avoids unspecified future work in the MVP. The only conditional work is Task 10, gated by measured `final_checkpoint_ms`.
- Type consistency: proto names use `ActorRoute`, `ActorMigration`, `RouteTarget`, and phase enums consistently across control API, router, and perfkit.
- Risk: Task 3 and Task 4 touch existing worker assignment and checkpoint code where the worktree already has unrelated changes. Implementation must read current files before editing and must not revert those changes.
