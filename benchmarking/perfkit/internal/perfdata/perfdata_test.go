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

package perfdata

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSummarizeLifecycleEvents(t *testing.T) {
	events := []LifecycleEvent{
		{Round: "r1", Operation: "create", ActorID: "a/1", Result: "ok", DurationMS: 100, StartTS: "2026-07-14T00:00:00Z", EndTS: "2026-07-14T00:00:00.100Z", NodeScale: 10, Concurrency: 1},
		{Round: "r1", Operation: "create", ActorID: "a/2", Result: "ok", DurationMS: 200, StartTS: "2026-07-14T00:00:01Z", EndTS: "2026-07-14T00:00:01.200Z", NodeScale: 10, Concurrency: 1},
		{Round: "r1", Operation: "resume_boot", ActorID: "a/2", Result: "error", ErrorCode: "FailedPrecondition", ErrorReason: "no free workers available", DurationMS: 5, StartTS: "2026-07-14T00:00:02Z", EndTS: "2026-07-14T00:00:02.005Z", NodeScale: 10, Concurrency: 1},
		{Round: "r2", Operation: "create", ActorID: "a/3", Result: "ok", DurationMS: 300, StartTS: "2026-07-14T00:01:00Z", EndTS: "2026-07-14T00:01:00.300Z", NodeScale: 50, Concurrency: 5},
	}

	summary := Summarize(events)

	if summary.EventCount != 4 {
		t.Fatalf("EventCount=%d, want 4", summary.EventCount)
	}
	r1 := summary.RoundByName("r1")
	if r1 == nil {
		t.Fatalf("missing r1 summary")
	}
	if r1.Actors != 2 || r1.Operations != 3 || r1.Success != 2 || r1.Errors != 1 {
		t.Fatalf("unexpected r1 summary: %+v", *r1)
	}
	create := summary.LatencyByRoundOperation("r1", "create")
	if create == nil {
		t.Fatalf("missing create latency")
	}
	if create.P50MS != 150 || create.P95MS != 195 || create.MaxMS != 200 {
		t.Fatalf("unexpected create percentiles: %+v", *create)
	}
	if len(summary.Errors) != 1 {
		t.Fatalf("errors=%d, want 1", len(summary.Errors))
	}
	if summary.Errors[0].Count != 1 || summary.Errors[0].Operation != "resume_boot" {
		t.Fatalf("unexpected error summary: %+v", summary.Errors[0])
	}
}

func TestReadLifecycleJSONL(t *testing.T) {
	input := strings.NewReader(`{"round":"r1","operation":"create","actor_id":"a/1","result":"ok","duration_ms":10}
{"round":"r1","operation":"delete","actor_id":"a/1","result":"ok","duration_ms":20}
`)
	events, err := ReadLifecycleJSONL(input, Defaults{Round: "fallback", NodeScale: 10, Concurrency: 1})
	if err != nil {
		t.Fatalf("ReadLifecycleJSONL returned error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events=%d, want 2", len(events))
	}
	if events[0].Round != "r1" || events[0].NodeScale != 10 || events[0].Concurrency != 1 {
		t.Fatalf("defaults not applied as expected: %+v", events[0])
	}
}

func TestReadLifecycleJSONLPreservesCrossNodeRecoverProof(t *testing.T) {
	input := strings.NewReader(`{"operation":"cross_node_recover","duration_ms":321,"source_worker":"ate-system/source","source_node":"node-a","target_worker":"ate-system/target","target_node":"node-b","snapshot_type":"external","recover_strategy":"source-node-drain","cross_node":true}` + "\n")

	events, err := ReadLifecycleJSONL(input, Defaults{Round: "cross-node-recover-con1", NodeScale: 10, Concurrency: 1})
	if err != nil {
		t.Fatalf("ReadLifecycleJSONL returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d, want 1", len(events))
	}
	ev := events[0]
	if !ev.CrossNode {
		t.Fatalf("CrossNode=false, want true")
	}
	if ev.SourceNode != "node-a" || ev.TargetNode != "node-b" {
		t.Fatalf("source/target node = %q/%q, want node-a/node-b", ev.SourceNode, ev.TargetNode)
	}
	if ev.SourceWorker != "ate-system/source" || ev.TargetWorker != "ate-system/target" {
		t.Fatalf("source/target worker = %q/%q, want ate-system/source/ate-system/target", ev.SourceWorker, ev.TargetWorker)
	}
	if ev.SnapshotType != "external" || ev.RecoverStrategy != "source-node-drain" {
		t.Fatalf("snapshot/strategy = %q/%q, want external/source-node-drain", ev.SnapshotType, ev.RecoverStrategy)
	}
}

func TestReadLifecycleJSONLPreservesHotMigrationProof(t *testing.T) {
	input := strings.NewReader(`{"operation":"hot_migration_summary","duration_ms":12345,"source_worker":"ate-system/source","source_node":"node-a","target_worker":"ate-system/target","target_node":"node-b","cross_node":true,"live_migration_downtime_ms":250,"live_migration_connections":8,"live_migration_memory_mode":"Precopy","migration_window_resource_result":"ok","end_to_end_resource_result":"pending_cleanup","route_generation_before":1,"route_generation_after":2,"http_counter":3,"http_state_version":4,"http_last_mutation_id":"increment-3","http_boot_id":"boot-a","http_pod_name":"pod-a","http_node_name":"node-b","http_route_generation":2,"http_route_phase":"PHASE_SWITCHED","http_route_target_worker":"ate-system/target","http_route_target_node":"node-b","http_route_target_ip":"10.0.0.2","longest_success_gap_ms":250,"max_request_start_gap_ms":110,"max_success_completion_gap_ms":560,"max_single_request_latency_ms":971,"total_to_target_success_ms":12345,"probe_requests":10,"probe_successes":10,"probe_canceled_by_stop":1,"missing_route_proof_count":0,"probe_status_counts":{"200":10},"mutation_requests":5,"mutation_successes":5,"mutation_failures":0,"mutation_canceled_by_stop":1,"mutation_max_accepted_counter":8,"mutation_duplicate_count":0,"mutation_lost_accepted_count":0,"monotonic_violation_count":0,"stale_version_count":0,"missing_state_proof_count":0,"route_switched_probe_requests":3,"route_switched_probe_successes":3,"service_errors_during_route_switched":0,"route_target_mismatch_during_route_switched":0,"missing_route_proof_during_route_switched":0,"mutation_route_switched_probe_requests":2,"mutation_route_switched_probe_successes":2,"mutation_service_errors_during_route_switched":0,"mutation_route_target_mismatch_during_route_switched":0,"mutation_missing_route_proof_during_route_switched":0}` + "\n")

	events, err := ReadLifecycleJSONL(input, Defaults{Round: "hot-migration-con1", NodeScale: 8, Concurrency: 1})
	if err != nil {
		t.Fatalf("ReadLifecycleJSONL returned error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events=%d, want 1", len(events))
	}
	ev := events[0]
	if ev.RouteGenerationBefore != 1 || ev.RouteGenerationAfter != 2 {
		t.Fatalf("route generation=%d/%d, want 1/2", ev.RouteGenerationBefore, ev.RouteGenerationAfter)
	}
	if ev.LiveMigrationDowntimeMS != 250 || ev.LiveMigrationConnections != 8 || ev.LiveMigrationMemoryMode != "Precopy" {
		t.Fatalf("live migration config proof not preserved: %+v", ev)
	}
	if ev.MigrationWindowResourceResult != "ok" || ev.EndToEndResourceResult != "pending_cleanup" {
		t.Fatalf("resource verdict proof not preserved: %+v", ev)
	}
	if ev.HTTPCounter != 3 || ev.HTTPStateVersion != 4 || ev.HTTPLastMutationID != "increment-3" || ev.HTTPBootID != "boot-a" {
		t.Fatalf("HTTP state proof not preserved: %+v", ev)
	}
	if ev.HTTPRouteGeneration != 2 || ev.HTTPRoutePhase != "PHASE_SWITCHED" || ev.HTTPRouteTargetWorker != "ate-system/target" || ev.HTTPRouteTargetNode != "node-b" || ev.HTTPRouteTargetIP != "10.0.0.2" {
		t.Fatalf("route trace proof not preserved: %+v", ev)
	}
	if ev.LongestSuccessGapMS != 250 || ev.MaxRequestStartGapMS != 110 || ev.MaxSuccessCompletionGapMS != 560 || ev.MaxSingleRequestLatencyMS != 971 || ev.TotalToTargetSuccessMS != 12345 {
		t.Fatalf("hot migration timing not preserved: %+v", ev)
	}
	if ev.ProbeStatusCounts["200"] != 10 {
		t.Fatalf("probe status counts=%v", ev.ProbeStatusCounts)
	}
	if ev.ProbeCanceledByStop != 1 || ev.MissingRouteProofCount != 0 {
		t.Fatalf("probe proof counters not preserved: %+v", ev)
	}
	if ev.MutationRequests != 5 || ev.MutationSuccesses != 5 || ev.MutationCanceledByStop != 1 || ev.MutationMaxAcceptedCounter != 8 {
		t.Fatalf("mutation counters not preserved: %+v", ev)
	}
	if ev.MutationFailures != 0 || ev.MutationDuplicateCount != 0 || ev.MutationLostAcceptedCount != 0 {
		t.Fatalf("mutation violation counters not preserved: %+v", ev)
	}
	if ev.MonotonicViolationCount != 0 || ev.StaleVersionCount != 0 || ev.MissingStateProofCount != 0 {
		t.Fatalf("consistency counters not preserved: %+v", ev)
	}
	if ev.RouteSwitchedProbeRequests != 3 || ev.RouteSwitchedProbeSuccesses != 3 || ev.ServiceErrorsDuringRouteSwitched != 0 || ev.RouteTargetMismatchDuringRouteSwitched != 0 || ev.MissingRouteProofDuringRouteSwitched != 0 {
		t.Fatalf("route-switched HTTP proof not preserved: %+v", ev)
	}
	if ev.MutationRouteSwitchedProbeRequests != 2 || ev.MutationRouteSwitchedProbeSuccesses != 2 || ev.MutationServiceErrorsDuringRouteSwitched != 0 || ev.MutationRouteTargetMismatchDuringRouteSwitched != 0 || ev.MutationMissingRouteProofDuringRouteSwitched != 0 {
		t.Fatalf("route-switched mutation proof not preserved: %+v", ev)
	}
}

func TestMarshalLifecycleEventKeepsZeroRouteSwitchedProofCounters(t *testing.T) {
	b, err := json.Marshal(LifecycleEvent{Operation: "hot_migration_summary"})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	got := string(b)
	for _, want := range []string{
		`"route_switched_probe_requests":0`,
		`"route_switched_probe_successes":0`,
		`"service_errors_during_route_switched":0`,
		`"route_target_mismatch_during_route_switched":0`,
		`"missing_route_proof_during_route_switched":0`,
		`"mutation_route_switched_probe_requests":0`,
		`"mutation_route_switched_probe_successes":0`,
		`"mutation_service_errors_during_route_switched":0`,
		`"mutation_route_target_mismatch_during_route_switched":0`,
		`"mutation_missing_route_proof_during_route_switched":0`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("marshaled event %s missing %s", got, want)
		}
	}
}

func TestRenderHTMLReportIncludesKeySections(t *testing.T) {
	summary := Summarize([]LifecycleEvent{
		{Round: "r2-con3", Operation: "create", ActorID: "a/1", Result: "ok", DurationMS: 42, NodeScale: 10, Concurrency: 3},
		{Round: "r2-con3", Operation: "resume_boot", ActorID: "a/1", Result: "ok", DurationMS: 84, NodeScale: 10, Concurrency: 3},
		{Round: "r2-con3", Operation: "resume_warm", ActorID: "a/1", Result: "ok", DurationMS: 63, NodeScale: 10, Concurrency: 3},
		{Round: "r2-con3", Operation: "suspend_1", ActorID: "a/1", Result: "ok", DurationMS: 51, NodeScale: 10, Concurrency: 3},
		{Round: "r2-con10", Operation: "resume_boot", ActorID: "a/2", Result: "error", ErrorCode: "FailedPrecondition", ErrorReason: "no free workers", DurationMS: 10, NodeScale: 10, Concurrency: 10},
	})
	html := RenderHTMLReport(summary, ReportMetadata{Title: "Substrate + gVisor ACK Performance Report", RunID: "run-1"})

	for _, want := range []string{
		"Substrate + gVisor ACK Performance Report",
		"执行摘要",
		"测试矩阵",
		"方法与口径",
		"指标定义",
		"瓶颈与异常分析",
		"原始数据与复现",
		"10 节点并发 3 稳态",
		"10 节点并发 10 容量探测",
		"生命周期吞吐（ops/s）",
		"生命周期操作结果分布",
		"冷恢复 p95",
		"轮次结果摘要",
		"生命周期延迟分位",
		"冷恢复 / 首次启动",
		"N/A",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("report missing %q in:\n%s", want, html)
		}
	}
	if got := strings.Count(html, "<svg"); got < 6 {
		t.Fatalf("report rendered %d charts, want at least 6", got)
	}
}
