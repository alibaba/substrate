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
