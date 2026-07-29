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
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/benchmarking/perfkit/internal/perfdata"
	"github.com/gorilla/websocket"
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
	body := []byte(`{"ready":true,"checksum":"abc123","resident_len":17179869184,"counter":7,"state_version":8,"last_mutation_id":"increment-7","instance_id":"instance-a","boot_id":"boot-a","pod_name":"pod-a","node_name":"node-a"}`)

	got := parseHTTPProbeBody(body)

	if got.Checksum != "abc123" {
		t.Fatalf("checksum=%q, want abc123", got.Checksum)
	}
	if got.Counter != 7 || got.InstanceID != "instance-a" || got.PodName != "pod-a" || got.NodeName != "node-a" {
		t.Fatalf("unexpected parsed probe metadata: %+v", got)
	}
	if got.StateVersion != 8 || got.LastMutationID != "increment-7" || got.BootID != "boot-a" {
		t.Fatalf("unexpected parsed state metadata: %+v", got)
	}
}

func TestApplyHTTPProbeHeadersExtractsRouteTrace(t *testing.T) {
	header := http.Header{}
	header.Set("X-Substrate-Route-Generation", "7")
	header.Set("X-Substrate-Route-Phase", "PHASE_SWITCHED")
	header.Set("X-Substrate-Route-Target-Worker", "ate-system/worker-b")
	header.Set("X-Substrate-Route-Target-Node", "node-b")
	header.Set("X-Substrate-Route-Target-IP", "10.0.0.2")

	result := httpProbeResult{}
	applyHTTPProbeHeaders(&result, header)

	if result.RouteGeneration != 7 || result.RoutePhase != "PHASE_SWITCHED" {
		t.Fatalf("route generation/phase = %d/%q, want 7/PHASE_SWITCHED", result.RouteGeneration, result.RoutePhase)
	}
	if result.RouteTargetWorker != "ate-system/worker-b" || result.RouteTargetNode != "node-b" || result.RouteTargetIP != "10.0.0.2" {
		t.Fatalf("route target = %q/%q/%q", result.RouteTargetWorker, result.RouteTargetNode, result.RouteTargetIP)
	}
}

func TestHTTPProbeContinuityRequiresSameChecksumWhenPresent(t *testing.T) {
	before := httpProbeResult{StatusCode: 200, Checksum: "before"}
	after := httpProbeResult{StatusCode: 200, Checksum: "after"}

	if err := validateHTTPProbeContinuity(before, after); err == nil {
		t.Fatal("validateHTTPProbeContinuity returned nil, want checksum mismatch error")
	}
}

func TestValidateExtraProbeStatsRejectsContinuityFailure(t *testing.T) {
	stats := continuousProbeStats{
		requests:  2,
		successes: 1,
		failures:  1,
		statusCounts: map[string]int{
			"200":   1,
			"error": 1,
		},
	}

	err := validateExtraProbeStats(extraProbeConfig{Label: "download", Path: "/download?bytes=1048576"}, stats, httpProbeResult{Counter: 1, StateVersion: 1})

	if err == nil {
		t.Fatal("validateExtraProbeStats returned nil, want failure")
	}
	if !strings.Contains(err.Error(), `extra probe "download"`) {
		t.Fatalf("error = %v, want extra probe label", err)
	}
}

func TestContinuousExtraProbeSummaryEventCarriesLabelAndPath(t *testing.T) {
	stats := continuousProbeStats{
		requests:  3,
		successes: 3,
		statusCounts: map[string]int{
			"200": 3,
		},
		longestSuccessGap:       200 * time.Millisecond,
		maxSingleRequestLatency: 150 * time.Millisecond,
	}

	ev := continuousExtraProbeSummaryEvent("run-a", "stage-a", "workload-a", "round-a", "atespace/actor-a", "actor-a", 0, 2, extraProbeConfig{Label: "download", Path: "/download?bytes=1048576"}, stats, nil)

	if ev.Operation != "continuous_extra_probe_summary" || ev.ProbeLabel != "download" || ev.ProbePath != "/download?bytes=1048576" {
		t.Fatalf("unexpected extra probe summary event: %+v", ev)
	}
	if ev.Result != "ok" || ev.ProbeRequests != 3 || ev.ProbeSuccesses != 3 || ev.MaxSingleRequestLatencyMS != 150 {
		t.Fatalf("unexpected extra probe summary stats: %+v", ev)
	}
}

func TestParsePreMigrationPostConfigs(t *testing.T) {
	got, err := parsePreMigrationPostConfigs([]string{"dirty=/dirty/start?rate_mib_per_s=128", "/substrate/quiesce"})
	if err != nil {
		t.Fatalf("parsePreMigrationPostConfigs: %v", err)
	}

	want := []preMigrationPostConfig{
		{Label: "dirty", Path: "/dirty/start?rate_mib_per_s=128"},
		{Label: "post-1", Path: "/substrate/quiesce"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pre-migration post configs = %+v, want %+v", got, want)
	}
}

func TestApplyHotMigrationConfigEvidenceRecordsLiveMigrationKnobs(t *testing.T) {
	cfg := HotMigrationConfig{
		LiveMigrationDowntimeMS:  250,
		LiveMigrationConnections: 8,
		LiveMigrationMemoryMode:  "Precopy",
	}
	ev := perfdata.LifecycleEvent{}

	applyHotMigrationConfigEvidence(&ev, cfg)

	if ev.LiveMigrationDowntimeMS != 250 {
		t.Fatalf("LiveMigrationDowntimeMS=%d, want 250", ev.LiveMigrationDowntimeMS)
	}
	if ev.LiveMigrationConnections != 8 {
		t.Fatalf("LiveMigrationConnections=%d, want 8", ev.LiveMigrationConnections)
	}
	if ev.LiveMigrationMemoryMode != "Precopy" {
		t.Fatalf("LiveMigrationMemoryMode=%q, want Precopy", ev.LiveMigrationMemoryMode)
	}
}

func TestHotMigrationResourceResultsDoNotMarkEndToEndOKBeforeCleanup(t *testing.T) {
	migrationWindowResource, endToEnd := hotMigrationResourceResults(false, nil)

	if migrationWindowResource != "ok" {
		t.Fatalf("migrationWindowResource=%q, want ok", migrationWindowResource)
	}
	if endToEnd != "pending_cleanup" {
		t.Fatalf("endToEnd=%q, want pending_cleanup", endToEnd)
	}
}

func TestHotMigrationResourceResultsMarkSkippedWhenFinalCleanupIsSkipped(t *testing.T) {
	migrationWindowResource, endToEnd := hotMigrationResourceResults(true, nil)

	if migrationWindowResource != "ok" {
		t.Fatalf("migrationWindowResource=%q, want ok", migrationWindowResource)
	}
	if endToEnd != "skipped" {
		t.Fatalf("endToEnd=%q, want skipped", endToEnd)
	}
}

func provenHTTPProbe(counter int64) httpProbeResult {
	return httpProbeResult{
		StatusCode:        http.StatusOK,
		Checksum:          "sum",
		Counter:           counter,
		StateVersion:      counter,
		LastMutationID:    "increment-" + strconv.FormatInt(counter, 10),
		RouteGeneration:   2,
		RouteTargetWorker: "ns/target",
		RouteTargetNode:   "node-b",
		RouteTargetIP:     "10.0.0.2",
	}
}

func TestValidateHotMigrationSummaryRequiresGenerationIncrease(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{node: "node-a"},
		workerPlacement{node: "node-b"},
		2,
		2,
		provenHTTPProbe(3),
		provenHTTPProbe(3),
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want generation error")
	}
}

func TestValidateHotMigrationSummaryRejectsStateVersionRegression(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{node: "node-a"},
		workerPlacement{node: "node-b"},
		2,
		3,
		httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 3, StateVersion: 9, LastMutationID: "increment-3"},
		httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 3, StateVersion: 8, LastMutationID: "increment-3", RouteTargetWorker: "ns/target", RouteTargetNode: "node-b", RouteTargetIP: "10.0.0.2"},
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want state regression error")
	}
}

func TestValidateHotMigrationSummaryRequiresCrossNodeProof(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{},
		workerPlacement{node: "node-b"},
		1,
		2,
		provenHTTPProbe(3),
		provenHTTPProbe(3),
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want missing cross-node proof error")
	}
}

func TestValidateHotMigrationSummaryRequiresRouteProof(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{namespace: "ns", pod: "source", node: "node-a"},
		workerPlacement{namespace: "ns", pod: "target", node: "node-b"},
		1,
		0,
		provenHTTPProbe(3),
		provenHTTPProbe(3),
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want missing after route generation proof error")
	}
}

func TestValidateHotMigrationSummaryRequiresStateProof(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{namespace: "ns", pod: "source", node: "node-a"},
		workerPlacement{namespace: "ns", pod: "target", node: "node-b"},
		1,
		2,
		httpProbeResult{StatusCode: 200, Checksum: "sum"},
		provenHTTPProbe(3),
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want missing state proof error")
	}
}

func TestValidateHotMigrationSummaryRequiresAfterRouteTargetToMatchMigrationTarget(t *testing.T) {
	err := validateHotMigrationSummary(
		workerPlacement{namespace: "ns", pod: "source", node: "node-a"},
		workerPlacement{namespace: "ns", pod: "target", node: "node-b"},
		1,
		2,
		provenHTTPProbe(3),
		httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 3, StateVersion: 3, LastMutationID: "increment-3", RouteTargetWorker: "ns/source", RouteTargetNode: "node-a", RouteTargetIP: "10.0.0.1"},
		true,
	)
	if err == nil {
		t.Fatal("validateHotMigrationSummary returned nil, want route target mismatch error")
	}
}

func TestValidateEndToEndResourceProofAcceptsFinalConsistentState(t *testing.T) {
	probe := provenHTTPProbe(9)
	probe.Ready = true
	probe.ResidentLen = 16 << 30
	probe.TargetMemoryBytes = 16 << 30

	if err := validateEndToEndResourceProof(probe, 9); err != nil {
		t.Fatalf("validateEndToEndResourceProof returned error: %v", err)
	}
}

func TestValidateEndToEndResourceProofRejectsMissingResidentMemory(t *testing.T) {
	probe := provenHTTPProbe(9)
	probe.Ready = true

	err := validateEndToEndResourceProof(probe, 9)
	if err == nil {
		t.Fatal("validateEndToEndResourceProof returned nil, want missing memory proof error")
	}
	if !strings.Contains(err.Error(), "resident memory proof") {
		t.Fatalf("error=%q, want resident memory proof", err)
	}
}

func TestValidateEndToEndResourceProofRejectsLostAcceptedMutation(t *testing.T) {
	probe := provenHTTPProbe(8)
	probe.Ready = true
	probe.ResidentLen = 16 << 30
	probe.TargetMemoryBytes = 16 << 30

	err := validateEndToEndResourceProof(probe, 9)
	if err == nil {
		t.Fatal("validateEndToEndResourceProof returned nil, want lost mutation error")
	}
	if !strings.Contains(err.Error(), "final counter") {
		t.Fatalf("error=%q, want final counter", err)
	}
}

func TestValidateContinuousProbeStatsRejectsStrongConsistencyViolations(t *testing.T) {
	stats := continuousProbeStats{
		requests:                8,
		successes:               6,
		failures:                2,
		monotonicViolationCount: 1,
		missingStateProofCount:  1,
		missingRouteProofCount:  1,
	}

	err := validateContinuousProbeStats(stats, 1, true)
	if err == nil {
		t.Fatal("validateContinuousProbeStats returned nil, want violation error")
	}
	msg := err.Error()
	for _, want := range []string{"failures=2", "monotonic_violations=1", "missing_state_proof=1", "missing_route_proof=1", "stale_versions=1", "dual_active=true"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q missing %q", msg, want)
		}
	}
}

func TestValidateContinuousProbeStatsAcceptsCleanProbeRun(t *testing.T) {
	stats := continuousProbeStats{
		requests:  8,
		successes: 8,
	}

	if err := validateContinuousProbeStats(stats, 0, false); err != nil {
		t.Fatalf("validateContinuousProbeStats returned %v, want nil", err)
	}
}

func TestPostSwitchIsolationStatsCountOnlySamplesAfterTargetSuccess(t *testing.T) {
	targetSuccess := time.Unix(100, 0)
	stats := continuousProbeStats{
		samples: []continuousProbeSample{
			{
				start:  targetSuccess.Add(-100 * time.Millisecond),
				end:    targetSuccess.Add(-50 * time.Millisecond),
				result: httpProbeResult{StatusCode: http.StatusOK},
			},
			{
				start:  targetSuccess.Add(100 * time.Millisecond),
				end:    targetSuccess.Add(150 * time.Millisecond),
				result: httpProbeResult{StatusCode: http.StatusOK},
			},
			{
				start: targetSuccess.Add(200 * time.Millisecond),
				end:   targetSuccess.Add(250 * time.Millisecond),
				err:   errors.New("router blackhole"),
			},
		},
	}

	got := postSwitchIsolationStats(stats, targetSuccess)

	if got.Requests != 2 {
		t.Fatalf("Requests=%d, want 2", got.Requests)
	}
	if got.Successes != 1 {
		t.Fatalf("Successes=%d, want 1", got.Successes)
	}
	if got.ServiceErrors != 1 {
		t.Fatalf("ServiceErrors=%d, want 1", got.ServiceErrors)
	}
}

func TestRouteSwitchedIsolationStatsCountServiceErrorsAndTargetMismatch(t *testing.T) {
	stats := continuousProbeStats{
		samples: []continuousProbeSample{
			{
				result: httpProbeResult{StatusCode: http.StatusOK, RoutePhase: "PHASE_DRAINING", RouteGeneration: 1, RouteTargetWorker: "ns/source", RouteTargetNode: "node-a", RouteTargetIP: "10.0.0.1"},
			},
			{
				result: httpProbeResult{StatusCode: http.StatusOK, RoutePhase: "PHASE_SWITCHED", RouteGeneration: 2, RouteTargetWorker: "ns/target", RouteTargetNode: "node-b", RouteTargetIP: "10.0.0.2"},
			},
			{
				result: httpProbeResult{StatusCode: http.StatusOK, RoutePhase: "PHASE_SWITCHED", RouteGeneration: 2, RouteTargetWorker: "ns/source", RouteTargetNode: "node-a", RouteTargetIP: "10.0.0.1"},
			},
			{
				result: httpProbeResult{StatusCode: http.StatusOK, RoutePhase: "PHASE_SWITCHED", RouteGeneration: 2, RouteTargetWorker: "ns/target", RouteTargetNode: "node-a", RouteTargetIP: "10.0.0.1"},
			},
			{
				err:    errors.New("blackhole"),
				result: httpProbeResult{RoutePhase: "PHASE_SWITCHED", RouteGeneration: 2, RouteTargetWorker: "ns/target", RouteTargetNode: "node-b", RouteTargetIP: "10.0.0.2"},
			},
			{
				result: httpProbeResult{StatusCode: http.StatusOK, RoutePhase: "PHASE_SWITCHED"},
			},
		},
	}

	got := routeSwitchedIsolationStats(stats, workerPlacement{namespace: "ns", pod: "target", node: "node-b"})

	if got.Requests != 5 {
		t.Fatalf("Requests=%d, want 5", got.Requests)
	}
	if got.Successes != 4 {
		t.Fatalf("Successes=%d, want 4", got.Successes)
	}
	if got.ServiceErrors != 1 {
		t.Fatalf("ServiceErrors=%d, want 1", got.ServiceErrors)
	}
	if got.TargetMismatches != 2 {
		t.Fatalf("TargetMismatches=%d, want 2", got.TargetMismatches)
	}
	if got.MissingRouteProof != 1 {
		t.Fatalf("MissingRouteProof=%d, want 1", got.MissingRouteProof)
	}
}

func TestValidateRouteSwitchedIsolationStatsRequiresSamplesWhenEnabled(t *testing.T) {
	err := validateRouteSwitchedIsolationStats(routeSwitchedIsolationProbeStats{}, true)

	if err == nil {
		t.Fatal("validateRouteSwitchedIsolationStats returned nil, want missing samples error")
	}
	if !strings.Contains(err.Error(), "route_switched_probe_requests=0") {
		t.Fatalf("error=%q, want route_switched_probe_requests=0", err)
	}
}

func TestValidateMutationRouteSwitchedIsolationStatsRequiresSamplesWhenEnabled(t *testing.T) {
	err := validateMutationRouteSwitchedIsolationStats(routeSwitchedIsolationProbeStats{}, true)

	if err == nil {
		t.Fatal("validateMutationRouteSwitchedIsolationStats returned nil, want missing samples error")
	}
	if !strings.Contains(err.Error(), "mutation_route_switched_probe_requests=0") {
		t.Fatalf("error=%q, want mutation_route_switched_probe_requests=0", err)
	}
}

func TestValidateMutationRouteSwitchedIsolationStatsReportsMutationViolations(t *testing.T) {
	stats := routeSwitchedIsolationProbeStats{
		Requests:          3,
		Successes:         1,
		ServiceErrors:     1,
		TargetMismatches:  1,
		MissingRouteProof: 1,
	}

	err := validateMutationRouteSwitchedIsolationStats(stats, true)
	if err == nil {
		t.Fatal("validateMutationRouteSwitchedIsolationStats returned nil, want violation error")
	}
	for _, want := range []string{
		"mutation_service_errors_during_route_switched=1",
		"mutation_route_target_mismatch_during_route_switched=1",
		"mutation_missing_route_proof_during_route_switched=1",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error=%q missing %q", err, want)
		}
	}
}

func TestObservationEndIncludesCleanupObservationWindow(t *testing.T) {
	targetSuccess := time.Unix(100, 0)
	cleanupEnd := targetSuccess.Add(60 * time.Second)

	if got := observationEnd(targetSuccess, cleanupEnd); !got.Equal(cleanupEnd) {
		t.Fatalf("observationEnd=%s, want cleanup end %s", got, cleanupEnd)
	}
	if got := observationEnd(targetSuccess, time.Time{}); !got.Equal(targetSuccess) {
		t.Fatalf("observationEnd without cleanup=%s, want target success %s", got, targetSuccess)
	}
}

func TestProbeStatsInWindowCountsOnlyCleanupWindow(t *testing.T) {
	cleanupStart := time.Unix(100, 0)
	cleanupEnd := cleanupStart.Add(10 * time.Second)
	stats := continuousProbeStats{samples: []continuousProbeSample{
		{start: cleanupStart.Add(-time.Second), end: cleanupStart.Add(-900 * time.Millisecond), result: httpProbeResult{StatusCode: http.StatusOK}},
		{start: cleanupStart.Add(time.Second), end: cleanupStart.Add(1100 * time.Millisecond), result: httpProbeResult{StatusCode: http.StatusOK}},
		{start: cleanupStart.Add(2 * time.Second), end: cleanupStart.Add(2100 * time.Millisecond), err: errors.New("boom")},
		{start: cleanupStart.Add(3 * time.Second), end: cleanupStart.Add(3100 * time.Millisecond), canceledByStop: true},
		{start: cleanupEnd.Add(time.Second), end: cleanupEnd.Add(1100 * time.Millisecond), result: httpProbeResult{StatusCode: http.StatusOK}},
	}}

	got := probeStatsInWindow(stats, cleanupStart, cleanupEnd)
	if got.Requests != 2 || got.Successes != 1 || got.ServiceErrors != 1 {
		t.Fatalf("probeStatsInWindow=%+v, want requests=2 successes=1 service_errors=1", got)
	}
}

func TestValidateContinuousMutationStatsRejectsLostDuplicateAndMissingProof(t *testing.T) {
	stats := continuousProbeStats{
		requests:               4,
		successes:              3,
		failures:               1,
		missingStateProofCount: 1,
		missingRouteProofCount: 1,
	}

	err := validateContinuousMutationStats(stats, 2, 1)
	if err == nil {
		t.Fatal("validateContinuousMutationStats returned nil, want violation error")
	}
	msg := err.Error()
	for _, want := range []string{"mutation_failures=1", "mutation_missing_state_proof=1", "mutation_missing_route_proof=1", "mutation_duplicate_counters=2", "mutation_lost_accepted=1"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q missing %q", msg, want)
		}
	}
}

func TestMutationCounterAccounting(t *testing.T) {
	samples := []continuousProbeSample{
		{result: httpProbeResult{StatusCode: http.StatusOK, Counter: 4}},
		{result: httpProbeResult{StatusCode: http.StatusOK, Counter: 5}},
		{result: httpProbeResult{StatusCode: http.StatusOK, Counter: 5}},
		{result: httpProbeResult{StatusCode: http.StatusOK, Counter: 7}},
		{result: httpProbeResult{StatusCode: http.StatusServiceUnavailable, Counter: 8}},
	}

	if got := countDuplicateSuccessCounters(samples); got != 1 {
		t.Fatalf("duplicate counters=%d, want 1", got)
	}
	if got := maxSuccessCounter(samples); got != 7 {
		t.Fatalf("max success counter=%d, want 7", got)
	}
	if got := countLostAcceptedMutations(samples, httpProbeResult{Counter: 6}); got != 1 {
		t.Fatalf("lost accepted mutations=%d, want 1", got)
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
		result:   httpProbeResult{StatusCode: 200, Checksum: "sum", Counter: 9, StateVersion: 10, LastMutationID: "increment-9", InstanceID: "instance-a", BootID: "boot-a", PodName: "pod-a", NodeName: "node-a"},
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
	if ev.HTTPStateVersion != 10 || ev.HTTPLastMutationID != "increment-9" || ev.HTTPInstanceID != "instance-a" || ev.HTTPBootID != "boot-a" || ev.HTTPPodName != "pod-a" || ev.HTTPNodeName != "node-a" {
		t.Fatalf("unexpected probe event state fields: %+v", ev)
	}
}

func TestContinuousProbeStartsRequestsAtFixedInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan time.Time, 10)
	probe := func(ctx context.Context) (httpProbeResult, error) {
		started <- time.Now()
		select {
		case <-ctx.Done():
			return httpProbeResult{}, ctx.Err()
		case <-time.After(250 * time.Millisecond):
			return httpProbeResult{StatusCode: http.StatusOK}, nil
		}
	}

	run := startContinuousProbeWithDo(ctx, probe, 50*time.Millisecond)
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(160 * time.Millisecond):
			t.Fatalf("probe %d did not start at fixed interval", i+1)
		}
	}

	cancel()
	<-run.done
}

func TestContinuousProbeIgnoresStopCanceledInflightRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{}, 1)
	probe := func(ctx context.Context) (httpProbeResult, error) {
		started <- struct{}{}
		<-ctx.Done()
		return httpProbeResult{}, ctx.Err()
	}

	run := startContinuousProbeWithDo(ctx, probe, time.Hour)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	cancel()
	stats := <-run.done

	if stats.requests != 0 || stats.successes != 0 || stats.failures != 0 || len(stats.samples) != 1 {
		t.Fatalf("stats = requests:%d successes:%d failures:%d samples:%d, want only one ignored sample", stats.requests, stats.successes, stats.failures, len(stats.samples))
	}
	if !stats.samples[0].canceledByStop {
		t.Fatalf("canceledByStop=false, want true: %+v", stats.samples[0])
	}
}

func TestContinuousProbeCountsCompletedErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	probe := func(ctx context.Context) (httpProbeResult, error) {
		cancel()
		return httpProbeResult{}, errors.New("connection refused")
	}

	run := startContinuousProbeWithDo(ctx, probe, time.Hour)
	stats := <-run.done

	if stats.requests != 1 || stats.successes != 0 || stats.failures != 1 || len(stats.samples) != 1 {
		t.Fatalf("stats = requests:%d successes:%d failures:%d samples:%d, want one counted failure", stats.requests, stats.successes, stats.failures, len(stats.samples))
	}
	if stats.samples[0].canceledByStop {
		t.Fatalf("canceledByStop=true for completed error: %+v", stats.samples[0])
	}
}

func TestContinuousProbeStatsTrackCompletionGapLatencyAndRegression(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := []httpProbeResult{
		{StatusCode: http.StatusOK, Counter: 2, StateVersion: 2, BootID: "boot-a"},
		{StatusCode: http.StatusOK, Counter: 1, StateVersion: 1, BootID: "boot-b"},
	}
	var calls int
	probe := func(ctx context.Context) (httpProbeResult, error) {
		if calls >= len(results) {
			cancel()
			return httpProbeResult{}, ctx.Err()
		}
		result := results[calls]
		calls++
		if calls == 2 {
			time.Sleep(30 * time.Millisecond)
			cancel()
		}
		return result, nil
	}

	run := startContinuousProbeWithDo(ctx, probe, 10*time.Millisecond)
	stats := <-run.done

	if stats.successes != 2 || stats.failures != 0 {
		t.Fatalf("successes=%d failures=%d, want 2/0", stats.successes, stats.failures)
	}
	if stats.maxRequestStartGap <= 0 {
		t.Fatalf("maxRequestStartGap=%v, want >0", stats.maxRequestStartGap)
	}
	if stats.maxSuccessCompletionGap <= 0 {
		t.Fatalf("maxSuccessCompletionGap=%v, want >0", stats.maxSuccessCompletionGap)
	}
	if stats.maxSingleRequestLatency <= 0 {
		t.Fatalf("maxSingleRequestLatency=%v, want >0", stats.maxSingleRequestLatency)
	}
	if stats.monotonicViolationCount != 1 {
		t.Fatalf("monotonicViolationCount=%d, want 1", stats.monotonicViolationCount)
	}
	if !dualActiveObserved(stats.samples) {
		t.Fatal("dualActiveObserved=false, want true for two boot IDs")
	}
	if stale := countStaleProbeVersions(stats.samples, httpProbeResult{Counter: 2, StateVersion: 2}); stale != 1 {
		t.Fatalf("stale versions=%d, want 1", stale)
	}
}

func TestContinuousProbeStatsIgnoresOverlappingProbeRegression(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := []struct {
		delay  time.Duration
		result httpProbeResult
	}{
		{delay: 60 * time.Millisecond, result: httpProbeResult{StatusCode: http.StatusOK, Counter: 2, StateVersion: 2, LastMutationID: "increment-2"}},
		{delay: 5 * time.Millisecond, result: httpProbeResult{StatusCode: http.StatusOK, Counter: 1, StateVersion: 1, LastMutationID: "increment-1"}},
	}
	var calls int
	probe := func(ctx context.Context) (httpProbeResult, error) {
		if calls >= len(results) {
			cancel()
			return httpProbeResult{}, ctx.Err()
		}
		item := results[calls]
		calls++
		time.Sleep(item.delay)
		if calls == len(results) {
			cancel()
		}
		return item.result, nil
	}

	run := startContinuousProbeWithDo(ctx, probe, 10*time.Millisecond)
	stats := <-run.done

	if stats.successes != 2 || stats.failures != 0 {
		t.Fatalf("successes=%d failures=%d, want 2/0", stats.successes, stats.failures)
	}
	if stats.monotonicViolationCount != 0 {
		t.Fatalf("monotonicViolationCount=%d, want 0 for overlapping requests", stats.monotonicViolationCount)
	}
}

func TestReadSSEProbeStatsTracksContinuityViolations(t *testing.T) {
	input := strings.NewReader(strings.Join([]string{
		`event: state`,
		`data: {"sequence":1,"state_version":3,"counter":3,"boot_id":"boot-a"}`,
		``,
		`event: state`,
		`data: {"sequence":3,"state_version":2,"counter":2,"boot_id":"boot-a"}`,
		``,
		`event: state`,
		`data: {"sequence":3,"state_version":4,"counter":4,"boot_id":"boot-a"}`,
		``,
	}, "\n"))

	ready := make(chan error, 1)
	stats := readSSEProbeStats(context.Background(), input, ready)

	if stats.events != 3 {
		t.Fatalf("events=%d, want 3", stats.events)
	}
	if err := <-ready; err != nil {
		t.Fatalf("ready err=%v, want nil after first event", err)
	}
	if stats.sequenceGapCount != 1 {
		t.Fatalf("sequenceGapCount=%d, want 1", stats.sequenceGapCount)
	}
	if stats.duplicateCount != 1 {
		t.Fatalf("duplicateCount=%d, want 1", stats.duplicateCount)
	}
	if stats.stateRegressionCount != 1 {
		t.Fatalf("stateRegressionCount=%d, want 1", stats.stateRegressionCount)
	}
	if stats.disconnects != 1 {
		t.Fatalf("disconnects=%d, want 1 because stream ended before context cancellation", stats.disconnects)
	}
}

func TestReadSSEProbeStatsRecordsMaxGapFrames(t *testing.T) {
	pr, pw := io.Pipe()
	ready := make(chan error, 1)
	done := make(chan sseProbeStats, 1)
	go func() {
		done <- readSSEProbeStats(context.Background(), pr, ready)
	}()

	if _, err := pw.Write([]byte("event: state\ndata: {\"sequence\":1,\"state_version\":1,\"counter\":1,\"boot_id\":\"boot-a\",\"server_unix_nano\":1000}\n\n")); err != nil {
		t.Fatalf("write first SSE frame: %v", err)
	}
	if err := <-ready; err != nil {
		t.Fatalf("ready err=%v, want nil after first event", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := pw.Write([]byte("event: state\ndata: {\"sequence\":2,\"state_version\":2,\"counter\":2,\"boot_id\":\"boot-a\",\"server_unix_nano\":2000}\n\n")); err != nil {
		t.Fatalf("write second SSE frame: %v", err)
	}
	_ = pw.Close()
	stats := <-done

	if stats.maxMessageGap <= 0 {
		t.Fatalf("maxMessageGap=%s, want positive gap", stats.maxMessageGap)
	}
	if stats.maxMessageGapPrevious.frame.Sequence != 1 || stats.maxMessageGapCurrent.frame.Sequence != 2 {
		t.Fatalf("max gap frames = previous:%+v current:%+v, want sequences 1 and 2", stats.maxMessageGapPrevious.frame, stats.maxMessageGapCurrent.frame)
	}
	if stats.maxMessageGapPrevious.frame.ServerUnixNano != 1000 || stats.maxMessageGapCurrent.frame.ServerUnixNano != 2000 {
		t.Fatalf("server timestamps not preserved in max gap frames: previous:%+v current:%+v", stats.maxMessageGapPrevious.frame, stats.maxMessageGapCurrent.frame)
	}
}

func TestSSEProbeSummaryTreatsContinuityViolationsAsError(t *testing.T) {
	cfg := HotMigrationConfig{
		LifecycleConfig: LifecycleConfig{
			RunID:       "run",
			Atespace:    "space",
			Stage:       "stage",
			Workload:    "memory-16g",
			Round:       "round",
			NodeScale:   2,
			Concurrency: 1,
		},
	}
	start := time.Date(2026, 7, 24, 7, 0, 0, 0, time.UTC)
	stats := sseProbeStats{
		events:               5,
		startedBeforePrepare: true,
		eventsBeforePrepare:  1,
		disconnects:          1,
		sequenceGapCount:     1,
		duplicateCount:       1,
		stateRegressionCount: 1,
	}

	ev := sseProbeSummaryEvent(cfg, "actor", 0, start, start.Add(time.Second), stats)

	if ev.Result != "error" {
		t.Fatalf("Result=%q, want error for SSE continuity violations: %+v", ev.Result, ev)
	}
	if ev.ErrorReason == "" {
		t.Fatalf("ErrorReason is empty, want continuity violation details: %+v", ev)
	}
}

func TestTerminalWebSocketSummaryTreatsContinuityViolationsAsError(t *testing.T) {
	cfg := HotMigrationConfig{
		LifecycleConfig: LifecycleConfig{
			RunID:       "run",
			Atespace:    "space",
			Stage:       "stage",
			Workload:    "memory-16g",
			Round:       "round",
			NodeScale:   2,
			Concurrency: 1,
		},
	}
	start := time.Date(2026, 7, 25, 7, 0, 0, 0, time.UTC)
	stats := terminalWebSocketStats{
		messages:             4,
		startedBeforePrepare: true,
		ticks:                2,
		echoes:               1,
		expectedEchoes:       3,
		disconnects:          1,
		sequenceGapCount:     1,
		missingEchoCount:     2,
		missingEchoSequences: []int{2, 3},
		sentEchoAt: map[int]time.Time{
			2: start.Add(150 * time.Millisecond),
			3: start.Add(350 * time.Millisecond),
		},
		pidChanged: true,
	}

	ev := terminalWebSocketSummaryEvent(cfg, "actor", 0, start, start.Add(time.Second), stats)

	if ev.Result != "error" {
		t.Fatalf("Result=%q, want error for terminal continuity violations: %+v", ev.Result, ev)
	}
	if ev.ErrorReason == "" {
		t.Fatalf("ErrorReason is empty, want continuity violation details: %+v", ev)
	}
	if ev.TerminalMessages != 4 || ev.TerminalTicks != 2 || ev.TerminalEchoes != 1 {
		t.Fatalf("terminal counters not propagated: %+v", ev)
	}
	if ev.TerminalExpectedEchoes != 3 {
		t.Fatalf("TerminalExpectedEchoes=%d, want 3", ev.TerminalExpectedEchoes)
	}
	if !reflect.DeepEqual(ev.TerminalMissingEchoSequences, []int{2, 3}) {
		t.Fatalf("TerminalMissingEchoSequences=%v, want [2 3]", ev.TerminalMissingEchoSequences)
	}
	if !reflect.DeepEqual(ev.TerminalMissingEchoSentOffsetsMS, []int64{150, 350}) {
		t.Fatalf("TerminalMissingEchoSentOffsetsMS=%v, want [150 350]", ev.TerminalMissingEchoSentOffsetsMS)
	}
	if ev.TerminalFirstMissingEchoSequence != 2 || ev.TerminalLastMissingEchoSequence != 3 {
		t.Fatalf("missing echo bounds not propagated: %+v", ev)
	}
	if ev.TerminalContinuityResult != "error" || ev.TerminalUXResult != "error" {
		t.Fatalf("terminal verdicts = %q/%q, want error/error", ev.TerminalContinuityResult, ev.TerminalUXResult)
	}
}

func TestTerminalWebSocketSummaryMarksUXDegradedWhenGapExceedsTarget(t *testing.T) {
	cfg := HotMigrationConfig{
		LifecycleConfig: LifecycleConfig{
			RunID:       "run",
			Atespace:    "space",
			Stage:       "stage",
			Workload:    "memory-16g",
			Round:       "round",
			NodeScale:   2,
			Concurrency: 1,
		},
	}
	start := time.Date(2026, 7, 25, 7, 0, 0, 0, time.UTC)
	stats := terminalWebSocketStats{
		messages:             4,
		startedBeforePrepare: true,
		ticks:                2,
		echoes:               2,
		expectedEchoes:       2,
		maxMessageGap:        1500 * time.Millisecond,
		echoLatencies:        []time.Duration{100 * time.Millisecond, 1200 * time.Millisecond},
	}

	ev := terminalWebSocketSummaryEvent(cfg, "actor", 0, start, start.Add(2*time.Second), stats)

	if ev.Result != "ok" {
		t.Fatalf("Result=%q, want continuity ok: %+v", ev.Result, ev)
	}
	if ev.TerminalContinuityResult != "ok" || ev.TerminalUXResult != "degraded" {
		t.Fatalf("terminal verdicts = %q/%q, want ok/degraded", ev.TerminalContinuityResult, ev.TerminalUXResult)
	}
	if ev.TerminalEchoLatencyMaxMS != 1200 || ev.TerminalEchoLatencyP95MS != 1200 {
		t.Fatalf("echo latency max/p95 = %d/%d, want 1200/1200", ev.TerminalEchoLatencyMaxMS, ev.TerminalEchoLatencyP95MS)
	}
	if ev.TerminalReconnectsObserved != 0 {
		t.Fatalf("TerminalReconnectsObserved=%d, want no real disconnects", ev.TerminalReconnectsObserved)
	}
	if !ev.TerminalUXGapObserved {
		t.Fatal("TerminalUXGapObserved=false, want true for a >1s gap")
	}
}

func TestTerminalWebSocketSummaryIncludesGapAndEchoTimingOffsets(t *testing.T) {
	cfg := HotMigrationConfig{
		LifecycleConfig: LifecycleConfig{
			RunID:       "run",
			Atespace:    "space",
			Stage:       "stage",
			Workload:    "memory-16g",
			Round:       "round",
			NodeScale:   2,
			Concurrency: 1,
		},
	}
	start := time.Date(2026, 7, 25, 7, 0, 0, 0, time.UTC)
	stats := terminalWebSocketStats{
		messages:                 4,
		startedBeforePrepare:     true,
		ticks:                    2,
		echoes:                   2,
		expectedEchoes:           2,
		maxMessageGap:            1500 * time.Millisecond,
		maxMessageGapStart:       start.Add(250 * time.Millisecond),
		maxMessageGapEnd:         start.Add(1750 * time.Millisecond),
		echoLatencies:            []time.Duration{100 * time.Millisecond, 1200 * time.Millisecond},
		maxEchoLatencySequence:   2,
		maxEchoLatencySentAt:     start.Add(400 * time.Millisecond),
		maxEchoLatencyReceivedAt: start.Add(1600 * time.Millisecond),
		maxMessageGapPrevious: terminalFrameSample{
			receivedAt: start.Add(250 * time.Millisecond),
			frame:      terminalFrame{Type: "tick", TerminalSequence: 7, Counter: 10, StateVersion: 10, LastMutationID: "increment-10", BootID: "boot-a", InstanceID: "inst-a", TerminalPID: 123, ServerUnixNano: 1000},
		},
		maxMessageGapCurrent: terminalFrameSample{
			receivedAt: start.Add(1750 * time.Millisecond),
			frame:      terminalFrame{Type: "echo", Input: "client-seq=2", Counter: 11, StateVersion: 11, LastMutationID: "increment-11", BootID: "boot-a", InstanceID: "inst-a", TerminalPID: 123, ServerUnixNano: 2000},
		},
		maxEchoLatencyFrame: terminalFrameSample{
			receivedAt: start.Add(1600 * time.Millisecond),
			frame:      terminalFrame{Type: "echo", Input: "client-seq=2", Counter: 11, StateVersion: 11, LastMutationID: "increment-11", BootID: "boot-a", InstanceID: "inst-a", TerminalPID: 123, ServerUnixNano: 1900},
		},
		stateRegressionCount: 1,
		stateRegressionSamples: []terminalStateRegressionSample{{
			previous: terminalFrameSample{
				receivedAt: start.Add(900 * time.Millisecond),
				frame:      terminalFrame{Type: "tick", TerminalSequence: 10, Counter: 12, StateVersion: 12, LastMutationID: "increment-12", BootID: "boot-a", InstanceID: "inst-a", TerminalPID: 123},
			},
			current: terminalFrameSample{
				receivedAt: start.Add(950 * time.Millisecond),
				frame:      terminalFrame{Type: "echo", Input: "client-seq=3", Counter: 11, StateVersion: 11, LastMutationID: "increment-11", BootID: "boot-a", InstanceID: "inst-a", TerminalPID: 123},
			},
		}},
	}

	ev := terminalWebSocketSummaryEvent(cfg, "actor", 0, start, start.Add(2*time.Second), stats)

	if ev.TerminalMaxMessageGapStartOffsetMS != 250 || ev.TerminalMaxMessageGapEndOffsetMS != 1750 {
		t.Fatalf("terminal max gap offsets = %d/%d, want 250/1750", ev.TerminalMaxMessageGapStartOffsetMS, ev.TerminalMaxMessageGapEndOffsetMS)
	}
	if ev.TerminalEchoLatencyMaxSequence != 2 {
		t.Fatalf("TerminalEchoLatencyMaxSequence=%d, want 2", ev.TerminalEchoLatencyMaxSequence)
	}
	if ev.TerminalEchoLatencyMaxSentOffsetMS != 400 || ev.TerminalEchoLatencyMaxReceivedOffsetMS != 1600 {
		t.Fatalf("terminal max echo offsets = %d/%d, want 400/1600", ev.TerminalEchoLatencyMaxSentOffsetMS, ev.TerminalEchoLatencyMaxReceivedOffsetMS)
	}
	if ev.TerminalMaxGapPreviousFrame == nil || ev.TerminalMaxGapPreviousFrame.Type != "tick" || ev.TerminalMaxGapPreviousFrame.StateVersion != 10 {
		t.Fatalf("missing max gap previous frame evidence: %+v", ev.TerminalMaxGapPreviousFrame)
	}
	if ev.TerminalMaxGapCurrentFrame == nil || ev.TerminalMaxGapCurrentFrame.ClientSequence != 2 || ev.TerminalMaxGapCurrentFrame.StateVersion != 11 {
		t.Fatalf("missing max gap current frame evidence: %+v", ev.TerminalMaxGapCurrentFrame)
	}
	if ev.TerminalEchoLatencyMaxFrame == nil || ev.TerminalEchoLatencyMaxFrame.ClientSequence != 2 {
		t.Fatalf("missing max echo latency frame evidence: %+v", ev.TerminalEchoLatencyMaxFrame)
	}
	if len(ev.TerminalStateRegressionSamples) != 1 {
		t.Fatalf("TerminalStateRegressionSamples len=%d, want 1", len(ev.TerminalStateRegressionSamples))
	}
	if sample := ev.TerminalStateRegressionSamples[0]; sample.Previous.StateVersion != 12 || sample.Current.StateVersion != 11 || sample.Current.ClientSequence != 3 {
		t.Fatalf("unexpected state regression evidence: %+v", sample)
	}
}

func TestParseTerminalClientSequenceAllowsTimingSuffix(t *testing.T) {
	seq, ok := parseTerminalClientSequence("client-seq=17 client_unix_nano=1785000000123456789")

	if !ok || seq != 17 {
		t.Fatalf("parseTerminalClientSequence()=%d/%v, want 17/true", seq, ok)
	}
}

func TestTerminalFrameEvidenceIncludesTimingBreakdown(t *testing.T) {
	start := time.Date(2026, 7, 25, 8, 0, 0, 0, time.UTC)
	sample := terminalFrameSample{
		receivedAt: start.Add(500 * time.Millisecond),
		frame: terminalFrame{
			Type:                     "echo",
			Input:                    "client-seq=3 client_unix_nano=1785000000000000000",
			ServerUnixNano:           1785000000100000000,
			ServerReadUnixNano:       1785000000001000000,
			ServerEnqueueUnixNano:    1785000000002000000,
			ServerWriteStartUnixNano: 1785000000100000000,
			ServerWriteEndUnixNano:   1785000000101000000,
		},
	}

	ev := terminalFrameEvidence(start, sample)

	if ev == nil {
		t.Fatal("terminalFrameEvidence returned nil")
	}
	if ev.ClientSequence != 3 {
		t.Fatalf("ClientSequence=%d, want 3", ev.ClientSequence)
	}
	if ev.ClientUnixNano != 1785000000000000000 {
		t.Fatalf("ClientUnixNano=%d, want timestamp from input", ev.ClientUnixNano)
	}
	if ev.ServerReadUnixNano != 1785000000001000000 ||
		ev.ServerEnqueueUnixNano != 1785000000002000000 ||
		ev.ServerWriteStartUnixNano != 1785000000100000000 ||
		ev.ServerWriteEndUnixNano != 1785000000101000000 {
		t.Fatalf("missing server timing breakdown: %+v", ev)
	}
}

func TestRouterHTTPServiceNameDefaultsToAtenetRouter(t *testing.T) {
	t.Setenv("PERFKIT_ROUTER_SERVICE_NAME", "")

	if got := routerHTTPServiceName(); got != "atenet-router" {
		t.Fatalf("routerHTTPServiceName() = %q, want atenet-router", got)
	}
}

func TestRouterHTTPServiceNameCanBeOverridden(t *testing.T) {
	t.Setenv("PERFKIT_ROUTER_SERVICE_NAME", "atenet-router-direct")

	if got := routerHTTPServiceName(); got != "atenet-router-direct" {
		t.Fatalf("routerHTTPServiceName() = %q, want atenet-router-direct", got)
	}
}

func TestRouterHTTPBaseURLCanBeOverridden(t *testing.T) {
	t.Setenv("PERFKIT_ROUTER_BASE_URL", "http://atenet-router-direct.ate-system.svc.cluster.local")

	if got := routerHTTPBaseURLOverride(); got != "http://atenet-router-direct.ate-system.svc.cluster.local" {
		t.Fatalf("routerHTTPBaseURLOverride() = %q", got)
	}
}

func TestRouterHTTPProberSendsNoCacheHeaders(t *testing.T) {
	var cacheControl, pragma string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		cacheControl = req.Header.Get("Cache-Control")
		pragma = req.Header.Get("Pragma")
		w.Header().Set("X-Substrate-Route-Generation", "1")
		w.Header().Set("X-Substrate-Route-Target-Worker", "ns/worker")
		w.Header().Set("X-Substrate-Route-Target-Node", "node-a")
		w.Header().Set("X-Substrate-Route-Target-IP", "10.0.0.1")
		_, _ = w.Write([]byte(`{"ready":true,"checksum":"sum","resident_len":1024,"target_memory_bytes":1024,"counter":1,"state_version":1,"last_mutation_id":"increment-1"}`))
	}))
	defer server.Close()

	prober := &routerHTTPProber{baseURL: server.URL, client: server.Client()}
	if _, err := prober.Probe(context.Background(), "space", "actor", "/state"); err != nil {
		t.Fatalf("Probe returned error: %v", err)
	}

	if cacheControl != "no-cache" || pragma != "no-cache" {
		t.Fatalf("cache headers=%q/%q, want no-cache/no-cache", cacheControl, pragma)
	}
}

func TestRouterHTTPProberWebSocketUsesActorHost(t *testing.T) {
	var gotHost string
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		gotHost = req.Host
		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"type": "tick", "terminal_sequence": 1})
	}))
	defer server.Close()

	prober := &routerHTTPProber{baseURL: server.URL, client: server.Client()}
	conn, err := prober.WebSocket(context.Background(), "space", "actor", "/terminal/ws")
	if err != nil {
		t.Fatalf("WebSocket returned error: %v", err)
	}
	defer conn.Close()

	var frame map[string]any
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read websocket frame: %v", err)
	}
	if gotHost != "actor.space.actors.resources.substrate.ate.dev" {
		t.Fatalf("websocket Host=%q, want actor DNS", gotHost)
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
