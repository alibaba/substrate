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
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func TestIsWorkerEligibleForActor(t *testing.T) {
	tests := []struct {
		name             string
		worker           *ateapipb.Worker
		templateClass    atev1alpha1.SandboxClass
		templateSelector *metav1.LabelSelector
		actorSelector    *ateapipb.Selector
		wantEligible     bool
	}{
		{
			name: "both nil matches everything",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"foo": "bar"},
			},
			templateClass:    atev1alpha1.SandboxClassGvisor,
			templateSelector: nil,
			actorSelector:    nil,
			wantEligible:     true,
		},
		{
			name: "template selector only match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "code-sandbox"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: nil,
			wantEligible:  true,
		},
		{
			name: "template selector only no match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "browser-agent"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: nil,
			wantEligible:  false,
		},
		{
			name: "actor selector only match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"tier": "paid"},
			},
			templateClass:    atev1alpha1.SandboxClassGvisor,
			templateSelector: nil,
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: true,
		},
		{
			name: "actor selector only no match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"tier": "free"},
			},
			templateClass:    atev1alpha1.SandboxClassGvisor,
			templateSelector: nil,
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: false,
		},
		{
			name: "AND of two selectors match",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "code-sandbox", "tier": "paid"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: true,
		},
		{
			name: "AND of two selectors one fails",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
				Labels:       map[string]string{"workload": "code-sandbox", "tier": "free"},
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			templateSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"workload": "code-sandbox"},
			},
			actorSelector: &ateapipb.Selector{
				MatchLabels: map[string]string{"tier": "paid"},
			},
			wantEligible: false,
		},
		{
			name: "microvm template matches only microvm worker",
			worker: &ateapipb.Worker{
				SandboxClass: "microvm",
			},
			templateClass: atev1alpha1.SandboxClassMicroVM,
			wantEligible:  true,
		},
		{
			name: "microvm template excludes gvisor worker",
			worker: &ateapipb.Worker{
				SandboxClass: "gvisor",
			},
			templateClass: atev1alpha1.SandboxClassMicroVM,
			wantEligible:  false,
		},
		{
			name: "gvisor template excludes microvm worker",
			worker: &ateapipb.Worker{
				SandboxClass: "microvm",
			},
			templateClass: atev1alpha1.SandboxClassGvisor,
			wantEligible:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isWorkerEligibleForActor(tt.worker, tt.templateClass, tt.templateSelector, tt.actorSelector)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantEligible {
				t.Errorf("got eligible=%t, want %t", got, tt.wantEligible)
			}
		})
	}
}

func TestAssignWorkerStep_SkipsWorkerAssignedInOtherAtespace(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	// The only worker is held by a same-named actor in another atespace. It is
	// eligible for the template, so a name-only match would adopt it.
	worker := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		SandboxClass:    "gvisor",
		Assignment: &ateapipb.Assignment{
			Actor: &ateapipb.ObjectRef{Atespace: "team-b", Name: "shared"},
		},
	}
	if err := persistence.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	cacheCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := workercache.New(persistence, time.Minute)
	if err := wc.Start(cacheCtx); err != nil {
		t.Fatalf("workercache.Start: %v", err)
	}

	step := &AssignWorkerStep{store: persistence, workerCache: wc}
	state := &ResumeState{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "shared"},
		},
		ActorTemplate: &atev1alpha1.ActorTemplate{
			Spec: atev1alpha1.ActorTemplateSpec{SandboxClass: atev1alpha1.SandboxClassGvisor},
		},
	}
	err := step.Execute(ctx, &ResumeInput{ActorName: "shared", Atespace: "team-a"}, state)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Execute() error = %v, want FailedPrecondition (no free workers)", err)
	}

	stored, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if got := stored.GetAssignment().GetActor().GetAtespace(); got != "team-b" {
		t.Errorf("worker assignment atespace = %q, want %q (assignment: %v)", got, "team-b", stored.GetAssignment())
	}
}

func TestRollbackFailedResumeReleasesWorkerAndRestoresSuspendedActor(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)

	worker := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool",
		WorkerPod:       "pod-1",
		Ip:              "10.0.0.1",
		WorkerPodUid:    "worker-uid",
		Assignment: &ateapipb.Assignment{
			Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		},
	}
	if err := persistence.CreateWorker(ctx, worker); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	actor := &ateapipb.Actor{
		Metadata:          &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status:            ateapipb.Actor_STATUS_RESUMING,
		AteomPodNamespace: "worker-ns",
		AteomPodName:      "pod-1",
		AteomPodIp:        "10.0.0.1",
		AteomPodUid:       "worker-uid",
		WorkerPoolName:    "pool",
		LatestSnapshotInfo: &ateapipb.SnapshotInfo{
			Data: &ateapipb.SnapshotInfo_External{
				External: &ateapipb.ExternalSnapshotInfo{SnapshotUriPrefix: "gs://bucket/snap"},
			},
		},
	}
	if _, err := persistence.CreateActor(ctx, actor); err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	if err := rollbackFailedResume(ctx, persistence, "team-a", "actor-1"); err != nil {
		t.Fatalf("rollbackFailedResume: %v", err)
	}

	storedWorker, err := persistence.GetWorker(ctx, "worker-ns", "pool", "pod-1")
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if storedWorker.GetAssignment() != nil {
		t.Fatalf("worker assignment = %v, want nil", storedWorker.GetAssignment())
	}

	storedActor, err := persistence.GetActor(ctx, "team-a", "actor-1")
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if storedActor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Fatalf("actor status = %v, want SUSPENDED", storedActor.GetStatus())
	}
	if storedActor.GetAteomPodName() != "" || storedActor.GetAteomPodNamespace() != "" || storedActor.GetWorkerPoolName() != "" {
		t.Fatalf("actor still has worker fields: %v", storedActor)
	}
	if storedActor.GetLatestSnapshotInfo().GetExternal().GetSnapshotUriPrefix() != "gs://bucket/snap" {
		t.Fatalf("latest snapshot was not preserved: %v", storedActor.GetLatestSnapshotInfo())
	}
}

func TestAssignWorkerStepFindFreeWorkerUsesStableOrder(t *testing.T) {
	step := &AssignWorkerStep{}
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "worker-ns-b",
			WorkerPod:       "pod-2",
			SandboxClass:    "gvisor",
			NodeName:        "node-2",
		},
		{
			WorkerNamespace: "worker-ns-a",
			WorkerPod:       "pod-1",
			SandboxClass:    "gvisor",
			NodeName:        "node-1",
		},
		{
			WorkerNamespace: "worker-ns-c",
			WorkerPod:       "pod-0",
			SandboxClass:    "gvisor",
			NodeName:        "node-3",
			Assignment: &ateapipb.Assignment{
				Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "busy"},
			},
		},
	}

	got, err := step.findFreeWorker(workers, atev1alpha1.SandboxClassGvisor, nil, nil, nil)
	if err != nil {
		t.Fatalf("findFreeWorker returned error: %v", err)
	}
	if got.GetWorkerPod() != "pod-1" || got.GetWorkerNamespace() != "worker-ns-a" {
		t.Fatalf("findFreeWorker picked %s/%s, want worker-ns-a/pod-1", got.GetWorkerNamespace(), got.GetWorkerPod())
	}
}

func TestAssignWorkerStepFindFreeWorkerRestrictsToLocalSnapshotNodes(t *testing.T) {
	step := &AssignWorkerStep{}
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "worker-ns",
			WorkerPod:       "pod-1",
			SandboxClass:    "gvisor",
			NodeName:        "node-without-snapshot",
		},
		{
			WorkerNamespace: "worker-ns",
			WorkerPod:       "pod-2",
			SandboxClass:    "gvisor",
			NodeName:        "node-with-snapshot",
		},
	}

	got, err := step.findFreeWorker(workers, atev1alpha1.SandboxClassGvisor, nil, nil, []string{"node-with-snapshot"})
	if err != nil {
		t.Fatalf("findFreeWorker returned error: %v", err)
	}
	if got.GetWorkerPod() != "pod-2" {
		t.Fatalf("findFreeWorker picked %s, want pod-2 on the local snapshot node", got.GetWorkerPod())
	}
}

func TestAssignWorkerStepPrefersExternalCacheNodeButFallsBack(t *testing.T) {
	step := &AssignWorkerStep{}
	workers := []*ateapipb.Worker{
		{WorkerNamespace: "worker-ns", WorkerPod: "pod-1", SandboxClass: "gvisor", NodeName: "node-fallback"},
		{WorkerNamespace: "worker-ns", WorkerPod: "pod-2", SandboxClass: "gvisor", NodeName: "node-cached"},
	}

	got, err := step.findFreeWorkerWithPreferences(workers, atev1alpha1.SandboxClassGvisor, nil, nil, nil, []string{"node-cached"}, "")
	if err != nil {
		t.Fatalf("findFreeWorkerWithPreferences returned error: %v", err)
	}
	if got.GetWorkerPod() != "pod-2" {
		t.Fatalf("preferred selection picked %s, want cached-node pod-2", got.GetWorkerPod())
	}

	workers[1].Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Atespace: "team", Name: "busy"}}
	got, err = step.findFreeWorkerWithPreferences(workers, atev1alpha1.SandboxClassGvisor, nil, nil, nil, []string{"node-cached"}, "")
	if err != nil {
		t.Fatalf("fallback selection returned error: %v", err)
	}
	if got.GetWorkerPod() != "pod-1" {
		t.Fatalf("fallback selection picked %s, want portable-snapshot pod-1", got.GetWorkerPod())
	}
}

func TestAssignWorkerStepFindFreeWorkerAvoidsNode(t *testing.T) {
	step := &AssignWorkerStep{}
	workers := []*ateapipb.Worker{
		{WorkerNamespace: "worker-ns", WorkerPod: "pod-1", SandboxClass: "gvisor", NodeName: "source-node"},
		{WorkerNamespace: "worker-ns", WorkerPod: "pod-2", SandboxClass: "gvisor", NodeName: "target-node"},
	}

	got, err := step.findFreeWorkerWithPreferences(workers, atev1alpha1.SandboxClassGvisor, nil, nil, nil, nil, "source-node")
	if err != nil {
		t.Fatalf("findFreeWorkerWithPreferences returned error: %v", err)
	}
	if got.GetWorkerPod() != "pod-2" {
		t.Fatalf("selection picked %s on avoided node, want pod-2", got.GetWorkerPod())
	}

	workers[1].Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Atespace: "team", Name: "busy"}}
	got, err = step.findFreeWorkerWithPreferences(workers, atev1alpha1.SandboxClassGvisor, nil, nil, nil, nil, "source-node")
	if err != nil {
		t.Fatalf("findFreeWorkerWithPreferences returned error with only avoided worker free: %v", err)
	}
	if got != nil {
		t.Fatalf("selection picked %s on avoided node, want no worker", got.GetWorkerPod())
	}
}

func TestWorkerSelectionStatsForActor(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "worker-ns",
			WorkerPod:       "assigned",
			SandboxClass:    "gvisor",
			NodeName:        "node-with-snapshot",
			Assignment: &ateapipb.Assignment{
				Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "busy"},
			},
		},
		{
			WorkerNamespace: "worker-ns",
			WorkerPod:       "restricted",
			SandboxClass:    "gvisor",
			NodeName:        "node-without-snapshot",
		},
		{
			WorkerNamespace: "worker-ns",
			WorkerPod:       "free",
			SandboxClass:    "gvisor",
			NodeName:        "node-with-snapshot",
		},
		{
			WorkerNamespace: "worker-ns",
			WorkerPod:       "wrong-class",
			SandboxClass:    "microvm",
			NodeName:        "node-with-snapshot",
		},
	}

	got, err := workerSelectionStatsForActor(workers, atev1alpha1.SandboxClassGvisor, nil, nil, []string{"node-with-snapshot"})
	if err != nil {
		t.Fatalf("workerSelectionStatsForActor returned error: %v", err)
	}
	want := workerSelectionStats{
		Total:              4,
		Assigned:           1,
		Eligible:           3,
		EligibleFree:       1,
		LocalityRestricted: 1,
		LocalSnapshotNodes: 1,
	}
	if got != want {
		t.Fatalf("workerSelectionStatsForActor = %+v, want %+v", got, want)
	}
}

func BenchmarkFindFreeWorkerLegacySelectorCompilation(b *testing.B) {
	step := &AssignWorkerStep{}
	workers := benchmarkWorkers(2000)
	templateSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"workload": "code-sandbox"}}
	actorSelector := &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	nodes := []string{"node-7", "node-9", "node-11"}

	b.ReportAllocs()
	for b.Loop() {
		worker, err := legacyFindFreeWorkerForBenchmark(workers, atev1alpha1.SandboxClassGvisor, templateSelector, actorSelector, nodes)
		if err != nil {
			b.Fatal(err)
		}
		if worker == nil {
			b.Fatal("expected worker")
		}
	}
	_ = step
}

func BenchmarkFindFreeWorkerPrecompiledSelector(b *testing.B) {
	step := &AssignWorkerStep{}
	workers := benchmarkWorkers(2000)
	templateSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"workload": "code-sandbox"}}
	actorSelector := &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}
	nodes := []string{"node-7", "node-9", "node-11"}

	b.ReportAllocs()
	for b.Loop() {
		worker, err := step.findFreeWorker(workers, atev1alpha1.SandboxClassGvisor, templateSelector, actorSelector, nodes)
		if err != nil {
			b.Fatal(err)
		}
		if worker == nil {
			b.Fatal("expected worker")
		}
	}
}

func benchmarkWorkers(n int) []*ateapipb.Worker {
	workers := make([]*ateapipb.Worker, 0, n)
	for i := 0; i < n; i++ {
		worker := &ateapipb.Worker{
			WorkerNamespace: "worker-ns",
			WorkerPod:       fmt.Sprintf("worker-%04d", i),
			SandboxClass:    "gvisor",
			NodeName:        fmt.Sprintf("node-%d", i%32),
			Labels: map[string]string{
				"workload": "code-sandbox",
				"tier":     "paid",
			},
		}
		if i%3 == 0 {
			worker.Assignment = &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Atespace: "team-a", Name: fmt.Sprintf("busy-%d", i)}}
		}
		workers = append(workers, worker)
	}
	return workers
}

func legacyFindFreeWorkerForBenchmark(
	workers []*ateapipb.Worker,
	templateClass atev1alpha1.SandboxClass,
	templateSelector *metav1.LabelSelector,
	actorSelector *ateapipb.Selector,
	nodesRestrictions []string,
) (*ateapipb.Worker, error) {
	var freeWorkers []*ateapipb.Worker
	for _, worker := range workers {
		if worker.Assignment != nil {
			continue
		}
		eligible, err := legacyIsWorkerEligibleForActorForBenchmark(worker, templateClass, templateSelector, actorSelector)
		if err != nil {
			return nil, err
		}
		if !eligible {
			continue
		}
		if len(nodesRestrictions) == 0 || slices.Contains(nodesRestrictions, worker.GetNodeName()) {
			freeWorkers = append(freeWorkers, worker)
		}
	}
	if len(freeWorkers) == 0 {
		return nil, nil
	}
	sort.Slice(freeWorkers, func(i, j int) bool {
		if freeWorkers[i].GetWorkerPod() != freeWorkers[j].GetWorkerPod() {
			return freeWorkers[i].GetWorkerPod() < freeWorkers[j].GetWorkerPod()
		}
		return freeWorkers[i].GetWorkerNamespace() < freeWorkers[j].GetWorkerNamespace()
	})
	return freeWorkers[0], nil
}

func legacyIsWorkerEligibleForActorForBenchmark(worker *ateapipb.Worker, templateClass atev1alpha1.SandboxClass, templateSelector *metav1.LabelSelector, actorSelector *ateapipb.Selector) (bool, error) {
	if worker.GetSandboxClass() != string(templateClass) {
		return false, nil
	}
	templateSel := labels.Everything()
	if templateSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(templateSelector)
		if err != nil {
			return false, fmt.Errorf("invalid template worker selector: %w", err)
		}
		templateSel = sel
	}
	actorSel := labels.SelectorFromSet(labels.Set(actorSelector.GetMatchLabels()))
	set := labels.Set(worker.GetLabels())
	return templateSel.Matches(set) && actorSel.Matches(set), nil
}
