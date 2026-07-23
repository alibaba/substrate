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

package runner

import (
	"testing"
	"time"

	"github.com/agent-substrate/substrate/benchmarking/perfkit/internal/perfdata"
)

func TestBlockerNameFitsActorNameLimit(t *testing.T) {
	parent := "xnode-smoke-000-1784099969339013901"

	got := blockerName(parent, 12)

	if len(got) > 63 {
		t.Fatalf("len(blockerName)=%d, want <=63: %q", len(got), got)
	}
	if got != "xnode-smoke-000-1784099969339013901-b12" {
		t.Fatalf("blockerName()=%q", got)
	}
}

func TestBlockerNameTruncatesLongParent(t *testing.T) {
	parent := "very-long-cross-node-recover-actor-name-that-would-overflow-when-used-for-a-blocker"

	got := blockerName(parent, 1)

	if len(got) > 63 {
		t.Fatalf("len(blockerName)=%d, want <=63: %q", len(got), got)
	}
	if got[len(got)-4:] != "-b01" {
		t.Fatalf("blockerName()=%q, want -b01 suffix", got)
	}
}

func TestPreBlockerNameUsesDistinctSuffix(t *testing.T) {
	parent := "xnode-smoke-000-1784099969339013901"

	got := preBlockerName(parent, 2)

	if len(got) > 63 {
		t.Fatalf("len(preBlockerName)=%d, want <=63: %q", len(got), got)
	}
	if got != "xnode-smoke-000-1784099969339013901-p02" {
		t.Fatalf("preBlockerName()=%q", got)
	}
}

func TestParseHTTPProbeBodyExtractsMemoryChecksum(t *testing.T) {
	body := []byte(`{"ready":true,"checksum":"abc123","resident_len":17179869184,"counter":7,"instance_id":"pod-a","node_name":"node-a"}`)

	got := parseHTTPProbeBody(body)

	if got.Checksum != "abc123" {
		t.Fatalf("checksum=%q, want abc123", got.Checksum)
	}
	if got.Counter != 7 || got.InstanceID != "pod-a" || got.NodeName != "node-a" {
		t.Fatalf("unexpected parsed probe metadata: %+v", got)
	}
}

func TestHTTPProbeContinuityRequiresSameChecksumWhenPresent(t *testing.T) {
	before := httpProbeResult{StatusCode: 200, Checksum: "before"}
	after := httpProbeResult{StatusCode: 200, Checksum: "after"}

	if err := validateHTTPProbeContinuity(before, after); err == nil {
		t.Fatal("validateHTTPProbeContinuity returned nil, want checksum mismatch error")
	}
}

func TestValidateHotMigrationSummaryRequiresGenerationIncrease(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{node: "node-a"},
		workerPlacement{node: "node-b"},
		2,
		2,
		httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 3},
		httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 3},
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want generation error")
	}
}

func TestApplyPlacementToEventMarksCrossNode(t *testing.T) {
	var ev perfdata.LifecycleEvent
	applyPlacementToEvent(&ev,
		workerPlacement{namespace: "ate-system", pod: "source", node: "node-a"},
		workerPlacement{namespace: "ate-system", pod: "target", node: "node-b"},
	)

	if !ev.CrossNode {
		t.Fatal("CrossNode=false, want true")
	}
	if ev.SourceWorker != "ate-system/source" || ev.TargetWorker != "ate-system/target" {
		t.Fatalf("unexpected workers: %+v", ev)
	}
}

func TestContinuousProbeSampleEventKeepsTimingAndStatus(t *testing.T) {
	start := time.Date(2026, 7, 23, 5, 4, 45, 100, time.UTC)
	end := start.Add(123 * time.Millisecond)
	sample := continuousProbeSample{
		sequence: 3,
		start:    start,
		end:      end,
		result:   httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 9},
	}

	ev := continuousProbeSampleEvent("run", "stage", "workload", "round", "space/actor", "actor", 0, 3, sample)

	if ev.Operation != "continuous_probe_sample" || ev.ProbeSequence != 3 {
		t.Fatalf("unexpected probe event identity: %+v", ev)
	}
	if ev.StartTS != "2026-07-23T05:04:45.0000001Z" || ev.DurationMS != 123 {
		t.Fatalf("unexpected probe event timing: %+v", ev)
	}
	if ev.Result != "ok" || ev.HTTPStatus != 200 || ev.HTTPChecksum != "sum" || ev.HTTPCounter != 9 {
		t.Fatalf("unexpected probe event HTTP fields: %+v", ev)
	}
}

func TestDrainBlockerLifecycleConfigDefaultsToMeasuredTemplate(t *testing.T) {
	cfg := CrossNodeRecoverConfig{
		LifecycleConfig: LifecycleConfig{
			ActorTemplateNamespace: "ate-demo-memory-gradient",
			ActorTemplateName:      "memory-16g",
		},
	}

	got := drainBlockerLifecycleConfig(cfg)

	if got.ActorTemplateNamespace != "ate-demo-memory-gradient" || got.ActorTemplateName != "memory-16g" {
		t.Fatalf("blocker template = %s/%s, want measured template", got.ActorTemplateNamespace, got.ActorTemplateName)
	}
}

func TestDrainBlockerLifecycleConfigUsesOverride(t *testing.T) {
	cfg := CrossNodeRecoverConfig{
		LifecycleConfig: LifecycleConfig{
			ActorTemplateNamespace: "ate-demo-memory-gradient",
			ActorTemplateName:      "memory-16g",
		},
		DrainBlockerActorTemplateNamespace: "ate-demo-counter",
		DrainBlockerActorTemplateName:      "counter",
	}

	got := drainBlockerLifecycleConfig(cfg)

	if got.ActorTemplateNamespace != "ate-demo-counter" || got.ActorTemplateName != "counter" {
		t.Fatalf("blocker template = %s/%s, want override", got.ActorTemplateNamespace, got.ActorTemplateName)
	}
}
