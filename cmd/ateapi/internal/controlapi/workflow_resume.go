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
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// ResumeInput holds the immutable parameters requested by the client.
type ResumeInput struct {
	ActorName     string
	Atespace      string
	Boot          bool
	AvoidNodeName string
}

// ResumeState holds the mutable state loaded and modified during execution.
type ResumeState struct {
	Actor         *ateapipb.Actor
	ActorTemplate *atev1alpha1.ActorTemplate
}

type LoadActorForResumeStep struct {
	store               store.Interface
	actorTemplateLister listersv1alpha1.ActorTemplateLister
}

func (s *LoadActorForResumeStep) Name() string { return "LoadActorForResume" }
func (s *LoadActorForResumeStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	// Always run this step to get the latest state from the DB
	return false, nil
}
func (s *LoadActorForResumeStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	actor, err := s.store.GetActor(ctx, input.Atespace, input.ActorName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.NotFound, "Actor %s not found", input.ActorName)
		}
		return fmt.Errorf("while getting actor from DB: %w", err)
	}
	state.Actor = actor

	actorTemplate, err := s.actorTemplateLister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		return fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	state.ActorTemplate = actorTemplate

	return nil
}

func (s *LoadActorForResumeStep) RetryBackoff() *wait.Backoff { return nil }

func isWorkerEligibleForActor(worker *ateapipb.Worker, templateClass atev1alpha1.SandboxClass, templateSelector *metav1.LabelSelector, actorSelector *ateapipb.Selector) (bool, error) {
	matcher, err := newWorkerEligibilityMatcher(templateClass, templateSelector, actorSelector)
	if err != nil {
		return false, err
	}
	return matcher.matches(worker), nil
}

type workerEligibilityMatcher struct {
	templateClass string
	templateSel   labels.Selector
	actorSel      labels.Selector
}

func newWorkerEligibilityMatcher(templateClass atev1alpha1.SandboxClass, templateSelector *metav1.LabelSelector, actorSelector *ateapipb.Selector) (*workerEligibilityMatcher, error) {
	// Snapshots are not portable across sandbox classes, so the worker's class
	// must match the template's. Both classes are populated by the CRD default
	// (gvisor), so we compare them directly.
	templateSel := labels.Everything()
	if templateSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(templateSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid template worker selector: %w", err)
		}
		templateSel = sel
	}

	actorSel := labels.SelectorFromSet(labels.Set(actorSelector.GetMatchLabels()))

	return &workerEligibilityMatcher{
		templateClass: string(templateClass),
		templateSel:   templateSel,
		actorSel:      actorSel,
	}, nil
}

func (m *workerEligibilityMatcher) matches(worker *ateapipb.Worker) bool {
	if worker.GetSandboxClass() != m.templateClass {
		return false
	}
	set := labels.Set(worker.GetLabels())
	return m.templateSel.Matches(set) && m.actorSel.Matches(set)
}

type AssignWorkerStep struct {
	store       store.Interface
	workerCache *workercache.Cache
}

func (s *AssignWorkerStep) Name() string { return "AssignWorker" }

func (s *AssignWorkerStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *AssignWorkerStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	workers, err := s.workerCache.Workers()
	if err != nil {
		return fmt.Errorf("while listing workers: %w", err)
	}

	var assignedWorker *ateapipb.Worker

	// Check if we already have a worker assigned from a previous failed attempt.
	// If that worker's pool is no longer eligible (e.g. the actor's
	// worker_selector was updated after the failed attempt), release it back
	// to the free pool instead of leaving it claimed forever — nothing else
	// reclaims a healthy worker whose actor moved on to a different pool.
	for _, worker := range workers {
		if worker.Assignment == nil {
			continue
		}
		if worker.Assignment.Actor.Atespace != input.Atespace || worker.Assignment.Actor.Name != input.ActorName {
			continue
		}
		eligible, err := isWorkerEligibleForActor(worker, state.ActorTemplate.Spec.SandboxClass, state.ActorTemplate.Spec.WorkerSelector, state.Actor.GetWorkerSelector())
		if err != nil {
			return fmt.Errorf("while checking worker eligibility: %w", err)
		}
		if eligible {
			assignedWorker = worker
			break
		}
		// Workers() returns pointers directly from the cache so we need to clone before
		// mutating so that the cache is not corrupted if UpdateWorker fails.
		release := proto.Clone(worker).(*ateapipb.Worker)
		release.Assignment = nil
		if err := s.store.UpdateWorker(ctx, release, release.Version); err != nil {
			return fmt.Errorf("while releasing stale worker assignment: %w", err)
		}
	}

	// If not, find a free one using randomized shuffling
	if assignedWorker == nil {
		hardNodes := state.Actor.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots()
		preferredNodes := state.Actor.GetLatestSnapshotInfo().GetExternal().GetNodeVmsWithLocalCache()
		pickedWorker, err := s.findFreeWorkerWithPreferences(workers, state.ActorTemplate.Spec.SandboxClass, state.ActorTemplate.Spec.WorkerSelector, state.Actor.GetWorkerSelector(), hardNodes, preferredNodes, input.AvoidNodeName)
		if err != nil {
			return err
		}
		if pickedWorker == nil {
			stats, statsErr := workerSelectionStatsForActor(workers, state.ActorTemplate.Spec.SandboxClass, state.ActorTemplate.Spec.WorkerSelector, state.Actor.GetWorkerSelector(), state.Actor.GetLatestSnapshotInfo().GetLocal().GetNodeVmsWithLocalSnapshots())
			if statsErr != nil {
				return statsErr
			}
			slog.WarnContext(ctx, "No free workers available",
				slog.Int("total_workers", stats.Total),
				slog.Int("assigned_workers", stats.Assigned),
				slog.Int("eligible_workers", stats.Eligible),
				slog.Int("eligible_free_workers", stats.EligibleFree),
				slog.Int("locality_restricted_workers", stats.LocalityRestricted),
				slog.Int("local_snapshot_nodes", stats.LocalSnapshotNodes))
			return status.Errorf(codes.FailedPrecondition, "no free workers available (total=%d assigned=%d eligible=%d eligible_free=%d locality_restricted=%d local_snapshot_nodes=%d)",
				stats.Total, stats.Assigned, stats.Eligible, stats.EligibleFree, stats.LocalityRestricted, stats.LocalSnapshotNodes)
		}

		assignedWorker = pickedWorker
		slog.InfoContext(ctx, "Picked worker", slog.Any("worker", pickedWorker.String()))
	}

	// Workers() returns pointers directly from the cache so we need to clone before
	// mutating so that the cache is not corrupted if UpdateWorker fails.
	assignedWorker = proto.Clone(assignedWorker).(*ateapipb.Worker)
	assignedWorker.Assignment = &ateapipb.Assignment{
		ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
			Namespace: state.Actor.GetActorTemplateNamespace(),
			Name:      state.Actor.GetActorTemplateName(),
		},
		Actor: &ateapipb.ObjectRef{
			Name:     input.ActorName,
			Atespace: state.Actor.GetMetadata().GetAtespace(),
		},
	}

	if err := s.store.UpdateWorker(ctx, assignedWorker, assignedWorker.Version); err != nil {
		return err
	}

	state.Actor.Status = ateapipb.Actor_STATUS_RESUMING
	state.Actor.AteomPodNamespace = assignedWorker.GetWorkerNamespace()
	state.Actor.AteomPodName = assignedWorker.GetWorkerPod()
	state.Actor.AteomPodIp = assignedWorker.GetIp()
	state.Actor.AteomPodUid = assignedWorker.GetWorkerPodUid()
	state.Actor.WorkerPoolName = assignedWorker.GetWorkerPool()

	updatedActor, err := s.store.UpdateActor(ctx, state.Actor, state.Actor.GetMetadata().GetVersion())
	if err != nil {
		return err
	}
	state.Actor = updatedActor
	return nil
}

func (s *AssignWorkerStep) RetryBackoff() *wait.Backoff {
	return &wait.Backoff{
		Steps:    5,
		Duration: 10 * time.Millisecond,
		Factor:   2.0,
		Jitter:   1.0,
	}
}

func (s *AssignWorkerStep) findFreeWorker(
	workers []*ateapipb.Worker,
	templateClass atev1alpha1.SandboxClass,
	templateSelector *metav1.LabelSelector,
	actorSelector *ateapipb.Selector,
	nodesRestrictions []string,
) (*ateapipb.Worker, error) {
	return s.findFreeWorkerWithPreferences(workers, templateClass, templateSelector, actorSelector, nodesRestrictions, nil, "")
}

func (s *AssignWorkerStep) findFreeWorkerWithPreferences(
	workers []*ateapipb.Worker,
	templateClass atev1alpha1.SandboxClass,
	templateSelector *metav1.LabelSelector,
	actorSelector *ateapipb.Selector,
	nodesRestrictions []string,
	preferredNodes []string,
	avoidNodeName string,
) (*ateapipb.Worker, error) {
	matcher, err := newWorkerEligibilityMatcher(templateClass, templateSelector, actorSelector)
	if err != nil {
		return nil, err
	}
	var freeWorkers []*ateapipb.Worker
	for _, worker := range workers {
		if worker.Assignment != nil {
			continue
		}
		if !matcher.matches(worker) {
			continue
		}
		if avoidNodeName != "" && worker.GetNodeName() == avoidNodeName {
			continue
		}
		if len(nodesRestrictions) == 0 || slices.Contains(nodesRestrictions, worker.GetNodeName()) {
			freeWorkers = append(freeWorkers, worker)
		}
	}

	if len(freeWorkers) > 0 {
		sort.Slice(freeWorkers, func(i, j int) bool {
			iPreferred := slices.Contains(preferredNodes, freeWorkers[i].GetNodeName())
			jPreferred := slices.Contains(preferredNodes, freeWorkers[j].GetNodeName())
			if iPreferred != jPreferred {
				return iPreferred
			}
			if freeWorkers[i].GetWorkerPod() != freeWorkers[j].GetWorkerPod() {
				return freeWorkers[i].GetWorkerPod() < freeWorkers[j].GetWorkerPod()
			}
			return freeWorkers[i].GetWorkerNamespace() < freeWorkers[j].GetWorkerNamespace()
		})
		return freeWorkers[0], nil
	}
	return nil, nil
}

type workerSelectionStats struct {
	Total              int
	Assigned           int
	Eligible           int
	EligibleFree       int
	LocalityRestricted int
	LocalSnapshotNodes int
}

func workerSelectionStatsForActor(
	workers []*ateapipb.Worker,
	templateClass atev1alpha1.SandboxClass,
	templateSelector *metav1.LabelSelector,
	actorSelector *ateapipb.Selector,
	nodesRestrictions []string,
) (workerSelectionStats, error) {
	matcher, err := newWorkerEligibilityMatcher(templateClass, templateSelector, actorSelector)
	if err != nil {
		return workerSelectionStats{}, err
	}
	stats := workerSelectionStats{
		Total:              len(workers),
		LocalSnapshotNodes: len(nodesRestrictions),
	}
	for _, worker := range workers {
		if worker.GetAssignment() != nil {
			stats.Assigned++
		}
		if !matcher.matches(worker) {
			continue
		}
		stats.Eligible++
		if worker.GetAssignment() != nil {
			continue
		}
		if len(nodesRestrictions) > 0 && !slices.Contains(nodesRestrictions, worker.GetNodeName()) {
			stats.LocalityRestricted++
			continue
		}
		stats.EligibleFree++
	}
	return stats, nil
}

type CallAteletRestoreStep struct {
	store               store.Interface
	dialer              *AteletDialer
	kubeClient          kubernetes.Interface
	secretCache         *envSecretCache
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
}

func (s *CallAteletRestoreStep) Name() string { return "CallAteletRestore" }
func (s *CallAteletRestoreStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *CallAteletRestoreStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	ateletConn, err := s.dialer.DialForWorker(state.Actor.GetAteomPodNamespace(), state.Actor.GetAteomPodName())
	if err != nil {
		return err
	}
	client := ateletpb.NewAteomHerderClient(ateletConn)

	workloadSpec, err := workloadSpecFromActorTemplateWithEnv(ctx, s.kubeClient, s.secretCache, state.ActorTemplate)
	if err != nil {
		return err
	}

	if data := state.Actor.GetLatestSnapshotInfo().GetData(); data != nil {
		slog.InfoContext(ctx, "Actor has snapshot; Restoring from snapshot")

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
		}
		switch d := data.(type) {
		case *ateapipb.SnapshotInfo_Local:
			req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			req.Config = &ateletpb.RestoreRequest_LocalConfig{
				LocalConfig: &ateletpb.LocalCheckpointConfiguration{
					SnapshotPrefix: d.Local.GetSnapshotPrefix(),
				},
			}
			req.Scope = toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnPause)
		case *ateapipb.SnapshotInfo_External:
			req.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL
			req.Config = &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: d.External.GetSnapshotUriPrefix(),
				},
			}
			req.Scope = toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnCommit)
		default:
			return fmt.Errorf("unsupported snapshot type: %T", data)
		}

		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorName, err, "while restoring workload")
	} else if state.ActorTemplate.Status.GoldenSnapshot != "" && !input.Boot {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has golden snapshot; Restoring from golden snapshot")

		snapshot := state.ActorTemplate.Status.GoldenSnapshot

		req := &ateletpb.RestoreRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			Spec:                   workloadSpec,
			Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
			Config: &ateletpb.RestoreRequest_ExternalConfig{
				ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
					SnapshotUriPrefix: snapshot,
				},
			},
			Scope: toAteletSnapshotScope(state.ActorTemplate.Spec.SnapshotsConfig.OnCommit),
		}
		_, err = client.Restore(ctx, req)
		return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorName, err, "while creating workload from golden snapshot")
	} else {
		slog.InfoContext(ctx, "Actor has no snapshot; ActorTemplate has no golden snapshot; Booting from ActorTemplate spec")

		// Booting from scratch: resolve the sandbox binaries from the pool's
		// SandboxConfig and send them so atelet can fetch and record them.
		// (Restores above are self-describing via the snapshot manifest.)
		sandboxAssets, err := resolveSandboxAssets(s.workerPoolLister, s.sandboxConfigLister, state.Actor.GetAteomPodNamespace(), state.Actor.GetWorkerPoolName())
		if err != nil {
			return fmt.Errorf("while resolving sandbox assets: %w", err)
		}

		req := &ateletpb.RunRequest{
			TargetAteomUid:         state.Actor.GetAteomPodUid(),
			Atespace:               state.Actor.GetMetadata().GetAtespace(),
			ActorName:              state.Actor.GetMetadata().GetName(),
			ActorTemplateNamespace: state.Actor.GetActorTemplateNamespace(),
			ActorTemplateName:      state.Actor.GetActorTemplateName(),
			SandboxAssets:          sandboxAssets,
			Spec:                   workloadSpec,
		}
		_, err = client.Run(ctx, req)
		return maybeCrashActor(ctx, s.store, input.Atespace, input.ActorName, err, "while creating workload from spec")
	}
	// Unreachable
}

func (s *CallAteletRestoreStep) RetryBackoff() *wait.Backoff { return nil }

type FinalizeRunningStep struct {
	store store.Interface
}

func (s *FinalizeRunningStep) Name() string { return "FinalizeRunning" }
func (s *FinalizeRunningStep) IsComplete(ctx context.Context, input *ResumeInput, state *ResumeState) (bool, error) {
	return state.Actor.GetStatus() == ateapipb.Actor_STATUS_RUNNING, nil
}
func (s *FinalizeRunningStep) Execute(ctx context.Context, input *ResumeInput, state *ResumeState) error {
	latestActor, err := s.store.GetActor(ctx, input.Atespace, input.ActorName)
	if err != nil {
		return err
	}

	latestActor.Status = ateapipb.Actor_STATUS_RUNNING
	updatedActor, err := s.store.UpdateActor(ctx, latestActor, latestActor.GetMetadata().GetVersion())
	if err != nil {
		return err
	}
	state.Actor = updatedActor
	return nil
}

func (s *FinalizeRunningStep) RetryBackoff() *wait.Backoff { return nil }

func shouldRollbackFailedResume(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.DataLoss:
		return true
	default:
		return false
	}
}

func rollbackFailedResume(ctx context.Context, st store.Interface, atespace, actorName string) error {
	actor, err := st.GetActor(ctx, atespace, actorName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("while loading actor for failed resume rollback: %w", err)
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RESUMING && actor.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		return nil
	}

	if actor.GetAteomPodNamespace() != "" && actor.GetAteomPodName() != "" && actor.GetWorkerPoolName() != "" {
		worker, err := st.GetWorker(ctx, actor.GetAteomPodNamespace(), actor.GetWorkerPoolName(), actor.GetAteomPodName())
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("while loading worker for failed resume rollback: %w", err)
			}
		} else if assignment := worker.GetAssignment(); assignment != nil &&
			assignment.GetActor().GetAtespace() == atespace &&
			assignment.GetActor().GetName() == actorName {
			worker.Assignment = nil
			if err := st.UpdateWorker(ctx, worker, worker.GetVersion()); err != nil {
				return fmt.Errorf("while releasing worker for failed resume rollback: %w", err)
			}
		}
	}

	if actor.GetStatus() == ateapipb.Actor_STATUS_RESUMING {
		if actor.GetLatestSnapshotInfo().GetData() != nil {
			actor.Status = ateapipb.Actor_STATUS_SUSPENDED
		} else {
			actor.Status = ateapipb.Actor_STATUS_CRASHED
		}
	}
	actor.AteomPodNamespace = ""
	actor.AteomPodName = ""
	actor.AteomPodIp = ""
	actor.AteomPodUid = ""
	actor.WorkerPoolName = ""
	if _, err := st.UpdateActor(ctx, actor, actor.GetMetadata().GetVersion()); err != nil {
		return fmt.Errorf("while updating actor for failed resume rollback: %w", err)
	}
	return nil
}
