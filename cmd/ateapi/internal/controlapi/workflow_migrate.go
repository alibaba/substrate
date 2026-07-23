// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validatePrepareMigrationActor(actor *ateapipb.Actor) error {
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		return status.Errorf(codes.FailedPrecondition, "actor must be RUNNING for hot migration, got %s", actor.GetStatus())
	}
	switch actor.GetMigration().GetPhase() {
	case ateapipb.ActorMigration_PHASE_UNSPECIFIED,
		ateapipb.ActorMigration_PHASE_COMMITTED,
		ateapipb.ActorMigration_PHASE_ABORTED,
		ateapipb.ActorMigration_PHASE_ROLLED_BACK:
		return nil
	default:
		return status.Errorf(codes.Aborted, "actor migration already in progress: %s", actor.GetMigration().GetPhase())
	}
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
	if source != nil {
		source = proto.Clone(source).(*ateapipb.RouteTarget)
	}
	candidate := target
	if candidate != nil {
		candidate = proto.Clone(candidate).(*ateapipb.RouteTarget)
	}
	actor.Route = &ateapipb.ActorRoute{
		Active:     source,
		Candidate:  candidate,
		Phase:      ateapipb.ActorRoute_PHASE_PREPARING,
		Generation: actor.GetRoute().GetGeneration(),
	}
	actor.Migration = &ateapipb.ActorMigration{
		MigrationId:           migrationID,
		Phase:                 ateapipb.ActorMigration_PHASE_PREPARING,
		Source:                source,
		Target:                candidate,
		BaseSnapshotUriPrefix: baseSnapshot,
		StageDurationMs:       map[string]int64{},
		StartTime:             now,
		UpdateTime:            now,
	}
}

func markMigrationTargetReady(actor *ateapipb.Actor) {
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_TARGET_READY
	actor.Migration.UpdateTime = timestamppb.Now()
}

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

func recordMigrationStageDuration(actor *ateapipb.Actor, stage string, d time.Duration) {
	if actor.GetMigration() == nil {
		return
	}
	if actor.Migration.StageDurationMs == nil {
		actor.Migration.StageDurationMs = map[string]int64{}
	}
	actor.Migration.StageDurationMs[stage] = d.Milliseconds()
}

func markMigrationAborted(actor *ateapipb.Actor) {
	actor.Route = &ateapipb.ActorRoute{
		Active:     proto.Clone(actor.GetRoute().GetActive()).(*ateapipb.RouteTarget),
		Phase:      ateapipb.ActorRoute_PHASE_ACTIVE,
		Generation: actor.GetRoute().GetGeneration(),
	}
	if actor.Migration == nil {
		actor.Migration = &ateapipb.ActorMigration{}
	}
	actor.Migration.Phase = ateapipb.ActorMigration_PHASE_ABORTED
	actor.Migration.UpdateTime = timestamppb.Now()
}

func (w *ActorWorkflow) PrepareActorMigration(ctx context.Context, atespace, name string, targetSelector *ateapipb.Selector, requireCrossNode bool) (*ateapipb.Actor, error) {
	ctx, releaseLock, err := w.acquireActorLock(ctx, atespace, name, 30*time.Second, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer releaseLock()

	actor, err := w.store.GetActor(ctx, atespace, name)
	if err != nil {
		return nil, err
	}
	if err := validatePrepareMigrationActor(actor); err != nil {
		return nil, err
	}

	actorTemplate, err := w.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}

	sourceWorker, err := w.store.GetWorker(ctx, actor.GetAteomPodNamespace(), actor.GetWorkerPoolName(), actor.GetAteomPodName())
	if err != nil {
		return nil, fmt.Errorf("while getting source worker: %w", err)
	}
	ensureActiveRoute(actor, sourceWorker.GetNodeName())

	candidate, err := w.pickMigrationTargetWorker(actor, actorTemplate, sourceWorker, targetSelector, requireCrossNode)
	if err != nil {
		return nil, err
	}
	target := routeTargetFromWorker(candidate)
	if err := validateMigrationTarget(actor.GetRoute().GetActive(), target, requireCrossNode); err != nil {
		return nil, err
	}

	baseSnapshot := ""
	reserveOnly := migrationPrepareReserveOnly() || migrationLiveCHEnabled()
	var prepareSourceCheckpoint time.Duration
	if !reserveOnly {
		baseSnapshot = migrationFinalSnapshot(actor, actorTemplate)
		stageStart := time.Now()
		if err := w.checkpointMigrationSource(ctx, actor, actorTemplate, actor.GetRoute().GetActive(), baseSnapshot, false, true); err != nil {
			return nil, err
		}
		prepareSourceCheckpoint = time.Since(stageStart)
	}

	candidate = proto.Clone(candidate).(*ateapipb.Worker)
	candidate.Assignment = &ateapipb.Assignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
			Namespace: actor.GetActorTemplateNamespace(),
			Name:      actor.GetActorTemplateName(),
		},
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     name,
		},
	}
	if err := w.store.UpdateWorker(ctx, candidate, candidate.GetVersion()); err != nil {
		return nil, err
	}

	markMigrationPreparing(actor, newMigrationID(), target, baseSnapshot)
	if !reserveOnly {
		recordMigrationStageDuration(actor, "prepare_source_checkpoint", prepareSourceCheckpoint)
	}
	stageStart := time.Now()
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}
	recordMigrationStageDuration(actor, "prepare_actor_update", time.Since(stageStart))

	if !reserveOnly {
		stageStart = time.Now()
		if err := w.restoreMigrationTarget(ctx, actor, actorTemplate, target); err != nil {
			return nil, err
		}
		recordMigrationStageDuration(actor, "prepare_target_restore", time.Since(stageStart))
	} else {
		slog.InfoContext(ctx, "Prepared actor migration target in reserve-only mode",
			slog.String("atespace", actor.GetMetadata().GetAtespace()),
			slog.String("actor", actor.GetMetadata().GetName()),
			slog.String("target_worker", target.GetAteomPodName()),
			slog.String("target_node", target.GetNodeName()))
	}

	markMigrationTargetReady(actor)
	stageStart = time.Now()
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}
	recordMigrationStageDuration(actor, "prepare_target_ready_update", time.Since(stageStart))
	return actor, nil
}

func (w *ActorWorkflow) pickMigrationTargetWorker(actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, sourceWorker *ateapipb.Worker, targetSelector *ateapipb.Selector, requireCrossNode bool) (*ateapipb.Worker, error) {
	workers, err := w.workerCache.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}
	selector, err := mergeSelectors(actor.GetWorkerSelector(), targetSelector)
	if err != nil {
		return nil, err
	}
	matcher, err := newWorkerEligibilityMatcher(actorTemplate.Spec.SandboxClass, actorTemplate.Spec.WorkerSelector, selector)
	if err != nil {
		return nil, err
	}
	for _, worker := range workers {
		if worker.GetAssignment() != nil {
			continue
		}
		if worker.GetWorkerNamespace() == sourceWorker.GetWorkerNamespace() &&
			worker.GetWorkerPool() == sourceWorker.GetWorkerPool() &&
			worker.GetWorkerPod() == sourceWorker.GetWorkerPod() {
			continue
		}
		if requireCrossNode && worker.GetNodeName() == sourceWorker.GetNodeName() {
			continue
		}
		if matcher.matches(worker) {
			return worker, nil
		}
	}
	return nil, status.Errorf(codes.FailedPrecondition, "no free migration target workers available")
}

func mergeSelectors(base, overlay *ateapipb.Selector) (*ateapipb.Selector, error) {
	merged := &ateapipb.Selector{MatchLabels: map[string]string{}}
	for k, v := range base.GetMatchLabels() {
		merged.MatchLabels[k] = v
	}
	for k, v := range overlay.GetMatchLabels() {
		if existing, ok := merged.MatchLabels[k]; ok && existing != v {
			return nil, status.Errorf(codes.FailedPrecondition, "conflicting target worker selector label %q", k)
		}
		merged.MatchLabels[k] = v
	}
	return merged, nil
}

func routeTargetFromWorker(worker *ateapipb.Worker) *ateapipb.RouteTarget {
	return &ateapipb.RouteTarget{
		AteomPodNamespace: worker.GetWorkerNamespace(),
		AteomPodName:      worker.GetWorkerPod(),
		AteomPodIp:        worker.GetIp(),
		AteomPodUid:       worker.GetWorkerPodUid(),
		WorkerPoolName:    worker.GetWorkerPool(),
		NodeName:          worker.GetNodeName(),
	}
}

func migrationBaseSnapshot(actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) string {
	if uri := actor.GetLatestSnapshotInfo().GetExternal().GetSnapshotUriPrefix(); uri != "" {
		return uri
	}
	if prefix := actor.GetLatestSnapshotInfo().GetLocal().GetSnapshotPrefix(); prefix != "" {
		return prefix
	}
	return actorTemplate.Status.GoldenSnapshot
}

func (w *ActorWorkflow) restoreMigrationTarget(ctx context.Context, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, target *ateapipb.RouteTarget) error {
	if baseSnapshot := actor.GetMigration().GetBaseSnapshotUriPrefix(); baseSnapshot != "" {
		return w.restoreMigrationTargetFromExternalSnapshot(ctx, actor, actorTemplate, target, baseSnapshot)
	}
	if data := actor.GetLatestSnapshotInfo().GetData(); data != nil {
		switch d := data.(type) {
		case *ateapipb.SnapshotInfo_Local:
			return w.restoreMigrationTargetFromLocalSnapshot(ctx, actor, actorTemplate, target, d.Local.GetSnapshotPrefix())
		case *ateapipb.SnapshotInfo_External:
			return w.restoreMigrationTargetFromExternalSnapshot(ctx, actor, actorTemplate, target, d.External.GetSnapshotUriPrefix())
		default:
			return fmt.Errorf("unsupported snapshot type: %T", data)
		}
	}
	if actorTemplate.Status.GoldenSnapshot != "" {
		return w.restoreMigrationTargetFromExternalSnapshot(ctx, actor, actorTemplate, target, actorTemplate.Status.GoldenSnapshot)
	}
	return status.Error(codes.FailedPrecondition, "actor has no snapshot or golden snapshot for migration prepare")
}

func (w *ActorWorkflow) restoreMigrationTargetFromExternalSnapshot(ctx context.Context, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, target *ateapipb.RouteTarget, snapshot string) error {
	req := &ateletpb.RestoreRequest{
		TargetAteomUid:         target.GetAteomPodUid(),
		Atespace:               actor.GetMetadata().GetAtespace(),
		ActorName:              actor.GetMetadata().GetName(),
		ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
		ActorTemplateName:      actor.GetActorTemplateName(),
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUriPrefix: snapshot},
		},
		Scope: toAteletSnapshotScope(actorTemplate.Spec.SnapshotsConfig.OnCommit),
	}
	return w.callMigrationTargetRestore(ctx, actor, actorTemplate, target, req)
}

func (w *ActorWorkflow) restoreMigrationTargetFromLocalSnapshot(ctx context.Context, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, target *ateapipb.RouteTarget, snapshotPrefix string) error {
	req := &ateletpb.RestoreRequest{
		TargetAteomUid:         target.GetAteomPodUid(),
		Atespace:               actor.GetMetadata().GetAtespace(),
		ActorName:              actor.GetMetadata().GetName(),
		ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
		ActorTemplateName:      actor.GetActorTemplateName(),
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL,
		Config: &ateletpb.RestoreRequest_LocalConfig{
			LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotPrefix: snapshotPrefix},
		},
		Scope: toAteletSnapshotScope(actorTemplate.Spec.SnapshotsConfig.OnPause),
	}
	return w.callMigrationTargetRestore(ctx, actor, actorTemplate, target, req)
}

func (w *ActorWorkflow) callMigrationTargetRestore(ctx context.Context, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, target *ateapipb.RouteTarget, req *ateletpb.RestoreRequest) error {
	ateletConn, err := w.dialer.DialForWorker(target.GetAteomPodNamespace(), target.GetAteomPodName())
	if err != nil {
		return err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplateWithEnv(ctx, w.kubeClient, w.secretCache, actorTemplate)
	if err != nil {
		return err
	}
	req.Spec = workloadSpec

	slog.InfoContext(ctx, "Restoring actor migration target",
		slog.String("atespace", actor.GetMetadata().GetAtespace()),
		slog.String("actor", actor.GetMetadata().GetName()),
		slog.String("target_worker", target.GetAteomPodName()),
		slog.String("target_node", target.GetNodeName()))
	_, err = client.Restore(ctx, req)
	if err != nil {
		return fmt.Errorf("while restoring migration target: %w", err)
	}
	return nil
}

func (w *ActorWorkflow) CommitActorMigration(ctx context.Context, atespace, name string, drainTimeout time.Duration, requireQuiesce bool) (*ateapipb.Actor, error) {
	ctx, releaseLock, err := w.acquireActorLock(ctx, atespace, name, 120*time.Second, 2*time.Second)
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

	source := proto.Clone(actor.GetMigration().GetSource()).(*ateapipb.RouteTarget)
	target := proto.Clone(actor.GetMigration().GetTarget()).(*ateapipb.RouteTarget)
	actorTemplate, err := w.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}

	stageStart := time.Now()
	markMigrationDraining(actor, drainTimeout)
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}
	recordMigrationStageDuration(actor, "commit_drain_update", time.Since(stageStart))

	stageStart = time.Now()
	markMigrationFinalizing(actor)
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}
	recordMigrationStageDuration(actor, "commit_finalizing_update", time.Since(stageStart))

	finalSnapshot := ""
	routeSwitched := false
	if migrationLiveCHEnabled() {
		stageStart = time.Now()
		var onTargetReady func() (*ateapipb.Actor, error)
		if migrationLiveEarlyRouteSwitch() {
			onTargetReady = func() (*ateapipb.Actor, error) {
				stageStart := time.Now()
				markMigrationSwitched(actor, finalSnapshot)
				updated, err := w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
				if err != nil {
					return nil, err
				}
				recordMigrationStageDuration(updated, "commit_early_route_switch_update", time.Since(stageStart))
				return updated, nil
			}
		}
		earlyActor, err := w.liveMigrateActor(ctx, actor, actorTemplate, source, target, onTargetReady)
		if err != nil {
			return nil, err
		}
		if earlyActor != nil {
			actor = earlyActor
			routeSwitched = true
		}
		recordMigrationStageDuration(actor, "commit_live_migration", time.Since(stageStart))
	} else if migrationCommitReusePreparedTarget() && actor.GetMigration().GetBaseSnapshotUriPrefix() != "" {
		finalSnapshot = actor.GetMigration().GetBaseSnapshotUriPrefix()
		recordMigrationStageDuration(actor, "commit_reuse_prepared_target", 0)
		slog.InfoContext(ctx, "Reusing prepared migration target without final checkpoint",
			slog.String("atespace", actor.GetMetadata().GetAtespace()),
			slog.String("actor", actor.GetMetadata().GetName()),
			slog.String("target_worker", target.GetAteomPodName()),
			slog.String("target_node", target.GetNodeName()),
			slog.String("snapshot", finalSnapshot))
	} else {
		finalSnapshot = migrationFinalSnapshot(actor, actorTemplate)
		stageStart = time.Now()
		if err := w.checkpointMigrationSource(ctx, actor, actorTemplate, source, finalSnapshot, requireQuiesce, false); err != nil {
			return nil, err
		}
		recordMigrationStageDuration(actor, "commit_source_checkpoint", time.Since(stageStart))
		stageStart = time.Now()
		if err := w.restoreMigrationTargetFromExternalSnapshot(ctx, actor, actorTemplate, target, finalSnapshot); err != nil {
			return nil, err
		}
		recordMigrationStageDuration(actor, "commit_target_restore", time.Since(stageStart))
	}
	if finalSnapshot != "" {
		actor.LatestSnapshotInfo = &ateapipb.SnapshotInfo{
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{SnapshotUriPrefix: finalSnapshot},
			},
		}
	}
	if !routeSwitched {
		stageStart = time.Now()
		markMigrationSwitched(actor, finalSnapshot)
		actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
		if err != nil {
			return nil, err
		}
		recordMigrationStageDuration(actor, "commit_route_switch_update", time.Since(stageStart))
	}

	stageStart = time.Now()
	if err := w.releaseMigrationSource(ctx, atespace, name, source); err != nil {
		return nil, err
	}
	recordMigrationStageDuration(actor, "commit_release_source", time.Since(stageStart))

	stageStart = time.Now()
	markMigrationCommitted(actor)
	actor, err = w.store.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion())
	if err != nil {
		return nil, err
	}
	recordMigrationStageDuration(actor, "commit_committed_update", time.Since(stageStart))
	return actor, nil
}

func migrationFinalSnapshot(actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate) string {
	snapshotID := time.Now().Format(time.RFC3339) + "-" + rand.Text()
	return strings.TrimSuffix(actorTemplate.Spec.SnapshotsConfig.Location, "/") + "/" + actor.GetMetadata().GetName() + "/" + snapshotID
}

func migrationPrepareReserveOnly() bool {
	return os.Getenv("ATE_MIGRATION_PREPARE_RESERVE_ONLY") == "1"
}

func migrationLiveCHEnabled() bool {
	return os.Getenv("ATE_MIGRATION_LIVE_CH") == "1"
}

func migrationLiveEarlyRouteSwitch() bool {
	return os.Getenv("ATE_MIGRATION_LIVE_EARLY_ROUTE_SWITCH") == "1"
}

func migrationCommitReusePreparedTarget() bool {
	return os.Getenv("ATE_MIGRATION_COMMIT_REUSE_PREPARED_TARGET") == "1"
}

func migrationLiveReceiverPort() int {
	return intEnv("ATE_MIGRATION_LIVE_PORT", 19000)
}

func migrationLiveReceiverWarmup() time.Duration {
	return time.Duration(intEnv("ATE_MIGRATION_LIVE_RECEIVER_WARMUP_MS", 500)) * time.Millisecond
}

func migrationLiveDowntimeMs() int64 {
	return int64(intEnv("ATE_MIGRATION_LIVE_DOWNTIME_MS", 200))
}

func migrationLiveTimeoutS() int64 {
	return int64(intEnv("ATE_MIGRATION_LIVE_TIMEOUT_S", 120))
}

func migrationLiveConnections() int64 {
	return int64(intEnv("ATE_MIGRATION_LIVE_CONNECTIONS", 4))
}

func migrationLiveMemoryMode() string {
	if v := os.Getenv("ATE_MIGRATION_LIVE_MEMORY_MODE"); v != "" {
		return v
	}
	return "Precopy"
}

func intEnv(name string, def int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

func (w *ActorWorkflow) liveMigrateActor(ctx context.Context, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, source, target *ateapipb.RouteTarget, onTargetReady func() (*ateapipb.Actor, error)) (*ateapipb.Actor, error) {
	workloadSpec, err := workloadSpecFromActorTemplateWithEnv(ctx, w.kubeClient, w.secretCache, actorTemplate)
	if err != nil {
		return nil, err
	}
	sandboxAssets, err := resolveSandboxAssets(w.workerPoolLister, w.sandboxConfigLister, target.GetAteomPodNamespace(), target.GetWorkerPoolName())
	if err != nil {
		return nil, fmt.Errorf("while resolving target sandbox assets: %w", err)
	}
	targetConn, err := w.dialer.DialForWorker(target.GetAteomPodNamespace(), target.GetAteomPodName())
	if err != nil {
		return nil, err
	}
	sourceConn, err := w.dialer.DialForWorker(source.GetAteomPodNamespace(), source.GetAteomPodName())
	if err != nil {
		return nil, err
	}
	targetClient := ateletpb.NewAteomHerderClient(targetConn)
	sourceClient := ateletpb.NewAteomHerderClient(sourceConn)

	port := migrationLiveReceiverPort()
	receiverURL := fmt.Sprintf("tcp:0.0.0.0:%d", port)
	destinationURL := fmt.Sprintf("tcp:%s:%d", target.GetAteomPodIp(), port)
	memoryMode := migrationLiveMemoryMode()

	migrationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type receiveResult struct {
		actor *ateapipb.Actor
		err   error
	}
	receiveCh := make(chan receiveResult, 1)
	go func() {
		_, err := targetClient.ReceiveLiveMigration(migrationCtx, &ateletpb.ReceiveLiveMigrationRequest{
			TargetAteomUid:         target.GetAteomPodUid(),
			Atespace:               actor.GetMetadata().GetAtespace(),
			ActorName:              actor.GetMetadata().GetName(),
			ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
			ActorTemplateName:      actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			SandboxAssets:          sandboxAssets,
			ReceiverUrl:            receiverURL,
			MemoryMode:             memoryMode,
		})
		var updated *ateapipb.Actor
		if err == nil && onTargetReady != nil {
			updated, err = onTargetReady()
		}
		receiveCh <- receiveResult{actor: updated, err: err}
	}()

	receiveDone := false
	var receivedActor *ateapipb.Actor
	if warmup := migrationLiveReceiverWarmup(); warmup > 0 {
		select {
		case result := <-receiveCh:
			if result.err != nil {
				return nil, fmt.Errorf("while preparing live migration receiver: %w", result.err)
			}
			receivedActor = result.actor
			receiveDone = true
		case <-time.After(warmup):
		case <-migrationCtx.Done():
			return nil, migrationCtx.Err()
		}
	}

	slog.InfoContext(ctx, "Sending Cloud Hypervisor live migration",
		slog.String("atespace", actor.GetMetadata().GetAtespace()),
		slog.String("actor", actor.GetMetadata().GetName()),
		slog.String("source_worker", source.GetAteomPodName()),
		slog.String("target_worker", target.GetAteomPodName()),
		slog.String("destination_url", destinationURL),
		slog.Int64("downtime_ms", migrationLiveDowntimeMs()),
		slog.Int64("connections", migrationLiveConnections()),
		slog.String("memory_mode", memoryMode))
	if _, err := sourceClient.SendLiveMigration(migrationCtx, &ateletpb.SendLiveMigrationRequest{
		TargetAteomUid:  source.GetAteomPodUid(),
		ActorName:       actor.GetMetadata().GetName(),
		DestinationUrl:  destinationURL,
		DowntimeMs:      migrationLiveDowntimeMs(),
		TimeoutS:        migrationLiveTimeoutS(),
		TimeoutStrategy: "Cancel",
		Connections:     migrationLiveConnections(),
		MemoryMode:      memoryMode,
	}); err != nil {
		return nil, fmt.Errorf("while sending live migration: %w", err)
	}

	if !receiveDone {
		select {
		case result := <-receiveCh:
			if result.err != nil {
				return nil, fmt.Errorf("while receiving live migration: %w", result.err)
			}
			receivedActor = result.actor
		case <-migrationCtx.Done():
			return nil, migrationCtx.Err()
		}
	}
	return receivedActor, nil
}

func (w *ActorWorkflow) checkpointMigrationSource(ctx context.Context, actor *ateapipb.Actor, actorTemplate *atev1alpha1.ActorTemplate, source *ateapipb.RouteTarget, snapshot string, requireQuiesce bool, preserveRunning bool) error {
	ateletConn, err := w.dialer.DialForWorker(source.GetAteomPodNamespace(), source.GetAteomPodName())
	if err != nil {
		return err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec := workloadSpecFromActorTemplate(actorTemplate)
	req := &ateletpb.CheckpointRequest{
		TargetAteomUid:         source.GetAteomPodUid(),
		Atespace:               actor.GetMetadata().GetAtespace(),
		ActorName:              actor.GetMetadata().GetName(),
		ActorTemplateNamespace: actor.GetActorTemplateNamespace(),
		ActorTemplateName:      actor.GetActorTemplateName(),
		Spec:                   workloadSpec,
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUriPrefix: snapshot},
		},
		Scope:           toAteletSnapshotScope(actorTemplate.Spec.SnapshotsConfig.OnCommit),
		PreserveRunning: preserveRunning,
	}
	slog.InfoContext(ctx, "Checkpointing actor migration source",
		slog.String("atespace", actor.GetMetadata().GetAtespace()),
		slog.String("actor", actor.GetMetadata().GetName()),
		slog.String("source_worker", source.GetAteomPodName()),
		slog.String("source_node", source.GetNodeName()),
		slog.String("snapshot", snapshot),
		slog.Bool("require_quiesce", requireQuiesce),
		slog.Bool("preserve_running", preserveRunning))
	_, err = client.Checkpoint(ctx, req)
	if err != nil {
		return fmt.Errorf("while checkpointing migration source: %w", err)
	}
	return nil
}

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

func newMigrationID() string {
	return uuid.NewString()
}

func (w *ActorWorkflow) releaseMigrationSource(ctx context.Context, atespace, actorName string, source *ateapipb.RouteTarget) error {
	if source.GetAteomPodNamespace() == "" || source.GetWorkerPoolName() == "" || source.GetAteomPodName() == "" {
		return nil
	}
	worker, err := w.store.GetWorker(ctx, source.GetAteomPodNamespace(), source.GetWorkerPoolName(), source.GetAteomPodName())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while loading migration source worker: %w", err)
	}
	if assignment := worker.GetAssignment(); assignment != nil &&
		assignment.GetActor().GetAtespace() == atespace &&
		assignment.GetActor().GetName() == actorName {
		worker.Assignment = nil
		if err := w.store.UpdateWorker(ctx, worker, worker.GetVersion()); err != nil {
			return fmt.Errorf("while releasing migration source worker: %w", err)
		}
	}
	return nil
}
