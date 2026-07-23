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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestPrepareMigrationRejectsNonRunningActor(t *testing.T) {
	actor := &ateapipb.Actor{Status: ateapipb.Actor_STATUS_SUSPENDED}
	err := validatePrepareMigrationActor(actor)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepareMigrationRequiresCrossNodeWhenRequested(t *testing.T) {
	source := &ateapipb.RouteTarget{NodeName: "node-a"}
	target := &ateapipb.RouteTarget{AteomPodIp: "10.0.0.11", NodeName: "node-a"}
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
