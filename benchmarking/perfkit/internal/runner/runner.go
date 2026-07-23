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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/benchmarking/perfkit/internal/perfdata"
	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	substrateclientset "github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

type LifecycleConfig struct {
	Kubeconfig             string
	RunID                  string
	Atespace               string
	Count                  int
	Concurrency            int
	NodeScale              int
	Round                  string
	Stage                  string
	Workload               string
	ActorPrefix            string
	ActorTemplateNamespace string
	ActorTemplateName      string
	PostWarmResumeSleep    time.Duration
	AsyncSuspend           bool
	CheckpointReadyTimeout time.Duration
}

type CrossNodeRecoverConfig struct {
	LifecycleConfig
	PreBlockers                        int
	HTTPProbe                          bool
	ProbePath                          string
	PostRecoverProbeTimeout            time.Duration
	DrainBlockerActorTemplateNamespace string
	DrainBlockerActorTemplateName      string
	CrossNodeResumeTargeting           bool
}

type HotMigrationConfig struct {
	LifecycleConfig
	ProbePath              string
	MutationPath           string
	StateMutations         int
	ProbeInterval          time.Duration
	DrainTimeout           time.Duration
	PostCommitProbeTimeout time.Duration
	ActorTemplateTimeout   time.Duration
	TargetWorkerLabels     map[string]string
	RequireCrossNode       bool
	RequireQuiesce         bool
	BootSource             bool
}

type CleanupEvent struct {
	Atespace string `json:"atespace"`
	Actor    string `json:"actor,omitempty"`
	Action   string `json:"action"`
	Result   string `json:"result"`
	Code     string `json:"code,omitempty"`
	Error    string `json:"error,omitempty"`
}

func RunCrossNodeRecover(ctx context.Context, cfg CrossNodeRecoverConfig, out io.Writer) error {
	base := cfg.LifecycleConfig
	if base.Kubeconfig == "" || base.RunID == "" || base.Atespace == "" {
		return fmt.Errorf("kubeconfig, run id, and atespace are required")
	}
	if base.Count < 1 {
		return fmt.Errorf("count must be >= 1")
	}
	if base.Concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	if base.Concurrency != 1 {
		return fmt.Errorf("cross-node recover currently requires --concurrency=1 to avoid placement interference")
	}
	if cfg.PreBlockers < 0 {
		return fmt.Errorf("preblockers must be >= 0")
	}
	if base.Round == "" {
		base.Round = "cross-node-recover-con1"
	}
	if base.Stage == "" {
		base.Stage = "ateapi_cross_node_recover"
	}
	if base.Workload == "" {
		base.Workload = "counter-smoke"
	}
	if base.ActorPrefix == "" {
		base.ActorPrefix = sanitizeName(base.Atespace)
	}
	if base.ActorTemplateNamespace == "" {
		base.ActorTemplateNamespace = "ate-demo-counter"
	}
	if base.ActorTemplateName == "" {
		base.ActorTemplateName = "counter"
	}
	if cfg.ProbePath == "" {
		cfg.ProbePath = "/"
	}
	if cfg.PostRecoverProbeTimeout < 0 {
		return fmt.Errorf("post recover probe timeout must be >= 0")
	}

	client, cleanup, err := newATEClient(ctx, base.Kubeconfig)
	if err != nil {
		return fmt.Errorf("new ate client: %w", err)
	}
	defer client.Close()
	defer cleanup()

	var prober *routerHTTPProber
	if cfg.HTTPProbe {
		prober, err = newRouterHTTPProber(ctx, base.Kubeconfig)
		if err != nil {
			return fmt.Errorf("new router http prober: %w", err)
		}
		defer prober.Close()
	}

	if _, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: base.Atespace}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create atespace: %w", err)
	}

	enc := json.NewEncoder(out)
	var encMu sync.Mutex
	var failures int
	for i := 0; i < base.Count; i++ {
		if runCrossNodeRecoverSample(ctx, client, prober, enc, &encMu, cfg, i) {
			failures++
		}
	}

	if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: base.Atespace}}); err != nil {
		return fmt.Errorf("delete atespace after cross-node recover run: %w", err)
	}
	if failures > 0 {
		return fmt.Errorf("cross-node recover completed with %d failed samples", failures)
	}
	return nil
}

func RunHotMigration(ctx context.Context, cfg HotMigrationConfig, out io.Writer) error {
	base := cfg.LifecycleConfig
	if base.RunID == "" || base.Atespace == "" {
		return fmt.Errorf("run id and atespace are required")
	}
	if base.Count < 1 {
		return fmt.Errorf("count must be >= 1")
	}
	if base.Concurrency < 1 {
		base.Concurrency = 1
	}
	if base.Concurrency != 1 {
		return fmt.Errorf("hot migration currently requires --concurrency=1 to keep source/target proof unambiguous")
	}
	if base.Round == "" {
		base.Round = "hot-migration-con1"
	}
	if base.Stage == "" {
		base.Stage = "ateapi_hot_migration"
	}
	if base.Workload == "" {
		base.Workload = "memory-16g"
	}
	if base.ActorPrefix == "" {
		base.ActorPrefix = sanitizeName(base.Atespace)
	}
	if base.ActorTemplateNamespace == "" {
		base.ActorTemplateNamespace = "ate-demo-memory-gradient"
	}
	if base.ActorTemplateName == "" {
		base.ActorTemplateName = "memory-16g"
	}
	if cfg.ProbePath == "" {
		cfg.ProbePath = "/substrate/migration-state"
	}
	if cfg.MutationPath == "" {
		cfg.MutationPath = "/increment"
	}
	if cfg.StateMutations <= 0 {
		cfg.StateMutations = 1
	}
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = 100 * time.Millisecond
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 3 * time.Second
	}
	if cfg.PostCommitProbeTimeout <= 0 {
		cfg.PostCommitProbeTimeout = 30 * time.Second
	}
	if cfg.ActorTemplateTimeout <= 0 {
		cfg.ActorTemplateTimeout = 30 * time.Minute
	}
	cfg.LifecycleConfig = base

	client, cleanup, err := newATEClient(ctx, base.Kubeconfig)
	if err != nil {
		return fmt.Errorf("new ate client: %w", err)
	}
	defer client.Close()
	defer cleanup()

	prober, err := newRouterHTTPProber(ctx, base.Kubeconfig)
	if err != nil {
		return fmt.Errorf("new router http prober: %w", err)
	}
	defer prober.Close()
	prober.client.Timeout = 6 * time.Second

	if !cfg.BootSource {
		if err := waitForActorTemplateGolden(ctx, base.Kubeconfig, base.ActorTemplateNamespace, base.ActorTemplateName, cfg.ActorTemplateTimeout); err != nil {
			return err
		}
	}

	if _, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: base.Atespace}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create atespace: %w", err)
	}

	enc := json.NewEncoder(out)
	var encMu sync.Mutex
	var failures int
	for i := 0; i < base.Count; i++ {
		if runHotMigrationSample(ctx, client, prober, enc, &encMu, cfg, i) {
			failures++
		}
	}

	if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: base.Atespace}}); err != nil {
		return fmt.Errorf("delete atespace after hot migration run: %w", err)
	}
	if failures > 0 {
		return fmt.Errorf("hot migration completed with %d failed samples", failures)
	}
	return nil
}

func RunLifecycle(ctx context.Context, cfg LifecycleConfig, out io.Writer) error {
	if cfg.Kubeconfig == "" || cfg.RunID == "" || cfg.Atespace == "" {
		return fmt.Errorf("kubeconfig, run id, and atespace are required")
	}
	if cfg.Count < 1 {
		return fmt.Errorf("count must be >= 1")
	}
	if cfg.Concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	if cfg.Round == "" {
		cfg.Round = cfg.Stage
	}
	if cfg.Stage == "" {
		cfg.Stage = "ateapi_persistent_client"
	}
	if cfg.Workload == "" {
		cfg.Workload = "counter-smoke"
	}
	if cfg.ActorPrefix == "" {
		cfg.ActorPrefix = sanitizeName(cfg.Atespace)
	}
	if cfg.ActorTemplateNamespace == "" {
		cfg.ActorTemplateNamespace = "ate-demo-counter"
	}
	if cfg.ActorTemplateName == "" {
		cfg.ActorTemplateName = "counter"
	}

	client, cleanup, err := newATEClient(ctx, cfg.Kubeconfig)
	if err != nil {
		return fmt.Errorf("new ate client: %w", err)
	}
	defer client.Close()
	defer cleanup()

	if _, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: cfg.Atespace}},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create atespace: %w", err)
	}

	enc := json.NewEncoder(out)
	var encMu sync.Mutex
	var failures int64
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if runActorLifecycle(ctx, client, enc, &encMu, cfg, i) {
					atomic.AddInt64(&failures, 1)
				}
			}
		}()
	}
	for i := 0; i < cfg.Count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: cfg.Atespace}}); err != nil {
		return fmt.Errorf("delete atespace after lifecycle run: %w", err)
	}
	if failures > 0 {
		return fmt.Errorf("lifecycle run completed with %d failed actor sequences", failures)
	}
	return nil
}

func runHotMigrationSample(ctx context.Context, client *ateclient.Client, prober *routerHTTPProber, enc *json.Encoder, encMu *sync.Mutex, cfg HotMigrationConfig, i int) bool {
	actor := fmt.Sprintf("%s-%03d-%d", cfg.ActorPrefix, i, time.Now().UnixNano())
	ref := &ateapipb.ObjectRef{Atespace: cfg.Atespace, Name: actor}
	failed := false
	created := false
	deleted := false
	var source workerPlacement
	var target workerPlacement
	var beforeProbe httpProbeResult
	var afterProbe httpProbeResult
	var routeGenerationBefore int64
	var routeGenerationAfter int64
	var migrationStart time.Time
	var commitEnd time.Time
	var prepareStageDurationMS map[string]int64
	var commitStageDurationMS map[string]int64

	write := func(ev perfdata.LifecycleEvent) {
		encMu.Lock()
		defer encMu.Unlock()
		_ = enc.Encode(ev)
	}
	record := func(op string, start, end time.Time, stageDurationMS map[string]int64, err error) {
		ev := perfdata.LifecycleEvent{
			RunID:           cfg.RunID,
			OperationID:     fmt.Sprintf("%s-%03d-%s", actor, i, op),
			ActorID:         cfg.Atespace + "/" + actor,
			Operation:       op,
			Stage:           cfg.Stage,
			StartTS:         start.UTC().Format(time.RFC3339Nano),
			EndTS:           end.UTC().Format(time.RFC3339Nano),
			DurationMS:      end.Sub(start).Milliseconds(),
			Workload:        cfg.Workload,
			Round:           cfg.Round,
			NodeScale:       cfg.NodeScale,
			Concurrency:     cfg.Concurrency,
			Result:          "ok",
			StageDurationMS: stageDurationMS,
		}
		applyPlacementToEvent(&ev, source, target)
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
			failed = true
		}
		write(ev)
	}
	recordHTTPProbe := func(op string, start, end time.Time, result httpProbeResult, err error) {
		ev := perfdata.LifecycleEvent{
			RunID:          cfg.RunID,
			OperationID:    fmt.Sprintf("%s-%03d-%s", actor, i, op),
			ActorID:        cfg.Atespace + "/" + actor,
			Operation:      op,
			Stage:          cfg.Stage,
			StartTS:        start.UTC().Format(time.RFC3339Nano),
			EndTS:          end.UTC().Format(time.RFC3339Nano),
			DurationMS:     end.Sub(start).Milliseconds(),
			Workload:       cfg.Workload,
			Round:          cfg.Round,
			NodeScale:      cfg.NodeScale,
			Concurrency:    cfg.Concurrency,
			Result:         "ok",
			HTTPStatus:     result.StatusCode,
			HTTPChecksum:   result.Checksum,
			HTTPCounter:    result.Counter,
			HTTPInstanceID: result.InstanceID,
			HTTPNodeName:   result.NodeName,
			HTTPBody:       result.Body,
			HTTPStale:      result.Stale,
		}
		applyPlacementToEvent(&ev, source, target)
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
			failed = true
		}
		write(ev)
	}
	recordContinuousProbeSamples := func(samples []continuousProbeSample) {
		for _, sample := range samples {
			write(continuousProbeSampleEvent(cfg.RunID, cfg.Stage, cfg.Workload, cfg.Round, cfg.Atespace+"/"+actor, actor, i, cfg.NodeScale, sample))
		}
	}

	start := time.Now()
	_, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: cfg.Atespace, Name: actor},
		ActorTemplateNamespace: cfg.ActorTemplateNamespace,
		ActorTemplateName:      cfg.ActorTemplateName,
	}})
	end := time.Now()
	if err == nil {
		created = true
	}
	record("create", start, end, nil, err)
	if err != nil {
		return failed
	}

	start = time.Now()
	_, err = client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref, Boot: cfg.BootSource})
	end = time.Now()
	resumeOp := "resume_from_golden"
	if cfg.BootSource {
		resumeOp = "resume_boot"
	}
	record(resumeOp, start, end, nil, err)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	source, err = currentPlacement(ctx, client, ref)
	if err != nil {
		record("capture_source_worker", time.Now(), time.Now(), nil, err)
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}

	start = time.Now()
	warmupProbe, err := waitForHTTPProbe(ctx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.PostCommitProbeTimeout)
	end = time.Now()
	recordHTTPProbe("http_probe_router_warmup", start, end, warmupProbe, err)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	for mutation := 0; mutation < cfg.StateMutations; mutation++ {
		start = time.Now()
		mutationProbe, mutateErr := prober.Do(ctx, http.MethodPost, cfg.Atespace, actor, cfg.MutationPath)
		end = time.Now()
		recordHTTPProbe(fmt.Sprintf("http_mutation_before_migration_%03d", mutation), start, end, mutationProbe, mutateErr)
		if mutateErr != nil {
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}

	start = time.Now()
	beforeProbe, err = prober.Probe(ctx, cfg.Atespace, actor, cfg.ProbePath)
	end = time.Now()
	recordHTTPProbe("http_probe_before_migration", start, end, beforeProbe, err)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}
	if route, routeErr := client.GetActorRoute(ctx, &ateapipb.GetActorRouteRequest{Actor: ref}); routeErr == nil {
		routeGenerationBefore = route.GetRoute().GetGeneration()
	}

	probeCtx, stopProbes := context.WithCancel(ctx)
	probes := startContinuousProbe(probeCtx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.ProbeInterval)
	migrationStart = time.Now()
	prepareResp, err := client.PrepareActorMigration(ctx, &ateapipb.PrepareActorMigrationRequest{
		Actor:                ref,
		TargetWorkerSelector: &ateapipb.Selector{MatchLabels: cfg.TargetWorkerLabels},
		RequireCrossNode:     cfg.RequireCrossNode,
	})
	prepareEnd := time.Now()
	if err == nil {
		target = placementFromRouteTarget(prepareResp.GetActor().GetRoute().GetCandidate())
		prepareStageDurationMS = cloneStageDurationMS(prepareResp.GetActor().GetMigration().GetStageDurationMs())
	}
	record("prepare_actor_migration", migrationStart, prepareEnd, prepareStageDurationMS, err)
	if err != nil {
		stopProbes()
		<-probes.done
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	commitStart := time.Now()
	commitResp, err := client.CommitActorMigration(ctx, &ateapipb.CommitActorMigrationRequest{
		Actor:          ref,
		DrainTimeoutMs: int32(cfg.DrainTimeout.Milliseconds()),
		RequireQuiesce: cfg.RequireQuiesce,
	})
	commitEnd = time.Now()
	if err == nil {
		target = placementFromRouteTarget(commitResp.GetActor().GetRoute().GetActive())
		routeGenerationAfter = commitResp.GetActor().GetRoute().GetGeneration()
		commitStageDurationMS = cloneStageDurationMS(commitResp.GetActor().GetMigration().GetStageDurationMs())
	}
	record("commit_actor_migration", commitStart, commitEnd, commitStageDurationMS, err)
	if err != nil {
		stopProbes()
		<-probes.done
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}
	if routeGenerationAfter == 0 {
		if route, routeErr := client.GetActorRoute(ctx, &ateapipb.GetActorRouteRequest{Actor: ref}); routeErr == nil {
			routeGenerationAfter = route.GetRoute().GetGeneration()
			target = placementFromRouteTarget(route.GetRoute().GetActive())
		}
	}
	if target.node == "" {
		if p, placementErr := currentPlacement(ctx, client, ref); placementErr == nil {
			target = p
		}
	}
	if cfg.RequireCrossNode && source.node != "" && target.node != "" && source.node == target.node {
		err = status.Errorf(codes.FailedPrecondition, "hot migration landed on same node %s", source.node)
		record("verify_cross_node_migration", time.Now(), time.Now(), nil, err)
		stopProbes()
		<-probes.done
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}

	afterProbe, err = waitForHTTPProbe(ctx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.PostCommitProbeTimeout)
	afterEnd := time.Now()
	recordHTTPProbe("http_probe_after_migration", commitEnd, afterEnd, afterProbe, err)
	stopProbes()
	stats := <-probes.done
	recordContinuousProbeSamples(stats.samples)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	summaryErr := validateHotMigrationSummary(source, target, routeGenerationBefore, routeGenerationAfter, beforeProbe, afterProbe, cfg.RequireCrossNode)
	if summaryErr != nil {
		failed = true
	}
	summary := perfdata.LifecycleEvent{
		RunID:                  cfg.RunID,
		OperationID:            fmt.Sprintf("%s-%03d-hot-migration-summary", actor, i),
		ActorID:                cfg.Atespace + "/" + actor,
		Operation:              "hot_migration_summary",
		Stage:                  cfg.Stage,
		StartTS:                migrationStart.UTC().Format(time.RFC3339Nano),
		EndTS:                  afterEnd.UTC().Format(time.RFC3339Nano),
		DurationMS:             afterEnd.Sub(migrationStart).Milliseconds(),
		Workload:               cfg.Workload,
		Round:                  cfg.Round,
		NodeScale:              cfg.NodeScale,
		Concurrency:            cfg.Concurrency,
		Result:                 "ok",
		HTTPStatus:             afterProbe.StatusCode,
		HTTPChecksum:           afterProbe.Checksum,
		HTTPCounter:            afterProbe.Counter,
		HTTPInstanceID:         afterProbe.InstanceID,
		HTTPNodeName:           afterProbe.NodeName,
		RouteGenerationBefore:  routeGenerationBefore,
		RouteGenerationAfter:   routeGenerationAfter,
		LongestSuccessGapMS:    stats.longestSuccessGap.Milliseconds(),
		TotalToTargetSuccessMS: afterEnd.Sub(migrationStart).Milliseconds(),
		ProbeRequests:          stats.requests,
		ProbeSuccesses:         stats.successes,
		ProbeFailures:          stats.failures,
		ProbeStatusCounts:      stats.statusCounts,
		StateRegressed:         afterProbe.Counter < beforeProbe.Counter,
		StageDurationMS:        commitStageDurationMS,
	}
	applyPlacementToEvent(&summary, source, target)
	if summaryErr != nil {
		summary.Result = "error"
		summary.ErrorCode = status.Code(summaryErr).String()
		summary.ErrorReason = summaryErr.Error()
	}
	write(summary)

	start = time.Now()
	_, err = client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	end = time.Now()
	record("suspend_after_migration", start, end, nil, err)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}

	start = time.Now()
	_, err = client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	end = time.Now()
	if err == nil || status.Code(err) == codes.NotFound {
		deleted = true
	}
	record("delete", start, end, nil, err)
	if created && !deleted {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
	}
	return failed
}

func cloneStageDurationMS(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func runCrossNodeRecoverSample(ctx context.Context, client *ateclient.Client, prober *routerHTTPProber, enc *json.Encoder, encMu *sync.Mutex, cfg CrossNodeRecoverConfig, i int) bool {
	actor := fmt.Sprintf("%s-%03d-%d", cfg.ActorPrefix, i, time.Now().UnixNano())
	ref := &ateapipb.ObjectRef{Atespace: cfg.Atespace, Name: actor}
	failed := false
	created := false
	deleted := false
	var preBlockers []string
	var blockers []string
	var source workerPlacement
	var target workerPlacement
	var beforeProbe httpProbeResult

	write := func(ev perfdata.LifecycleEvent) {
		encMu.Lock()
		defer encMu.Unlock()
		_ = enc.Encode(ev)
	}
	record := func(op string, start, end time.Time, err error) {
		ev := perfdata.LifecycleEvent{
			RunID:       cfg.RunID,
			OperationID: fmt.Sprintf("%s-%03d-%s", actor, i, op),
			ActorID:     cfg.Atespace + "/" + actor,
			Operation:   op,
			Stage:       cfg.Stage,
			StartTS:     start.UTC().Format(time.RFC3339Nano),
			EndTS:       end.UTC().Format(time.RFC3339Nano),
			DurationMS:  end.Sub(start).Milliseconds(),
			Workload:    cfg.Workload,
			Round:       cfg.Round,
			NodeScale:   cfg.NodeScale,
			Concurrency: cfg.Concurrency,
			Result:      "ok",
		}
		if source.node != "" {
			ev.SourceWorker = source.key()
			ev.SourceNode = source.node
		}
		if target.node != "" {
			ev.TargetWorker = target.key()
			ev.TargetNode = target.node
			ev.CrossNode = source.node != "" && source.node != target.node
		}
		if op == "suspend_external" || op == "cross_node_recover" {
			ev.SnapshotType = "external"
		}
		if op == "cross_node_recover" {
			ev.RecoverStrategy = "source-node-drain"
			if cfg.CrossNodeResumeTargeting {
				ev.RecoverStrategy = "avoid-source-node"
			}
			if cfg.DrainBlockerActorTemplateNamespace != "" || cfg.DrainBlockerActorTemplateName != "" {
				ev.RecoverStrategy = "source-node-drain-light-blocker"
			}
			ev.BlockerActors = len(preBlockers) + len(blockers)
		}
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
			failed = true
		}
		write(ev)
	}
	recordHTTPProbe := func(op string, start, end time.Time, result httpProbeResult, err error) {
		ev := perfdata.LifecycleEvent{
			RunID:        cfg.RunID,
			OperationID:  fmt.Sprintf("%s-%03d-%s", actor, i, op),
			ActorID:      cfg.Atespace + "/" + actor,
			Operation:    op,
			Stage:        cfg.Stage,
			StartTS:      start.UTC().Format(time.RFC3339Nano),
			EndTS:        end.UTC().Format(time.RFC3339Nano),
			DurationMS:   end.Sub(start).Milliseconds(),
			Workload:     cfg.Workload,
			Round:        cfg.Round,
			NodeScale:    cfg.NodeScale,
			Concurrency:  cfg.Concurrency,
			Result:       "ok",
			HTTPStatus:   result.StatusCode,
			HTTPChecksum: result.Checksum,
			HTTPBody:     result.Body,
			HTTPStale:    result.Stale,
		}
		if source.node != "" {
			ev.SourceWorker = source.key()
			ev.SourceNode = source.node
		}
		if target.node != "" {
			ev.TargetWorker = target.key()
			ev.TargetNode = target.node
			ev.CrossNode = source.node != "" && source.node != target.node
		}
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
			failed = true
		}
		write(ev)
	}

	defer func() {
		cleanupBlockers(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, preBlockers)
	}()

	for attempt := 0; attempt < cfg.PreBlockers; attempt++ {
		preBlocker := preBlockerName(actor, attempt)
		preBlockerRef := &ateapipb.ObjectRef{Atespace: cfg.Atespace, Name: preBlocker}
		preBlockers = append(preBlockers, preBlocker)
		if err := runHeldActorCreateAndResume(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, preBlocker, preBlockerRef, "preblocker"); err != nil {
			return true
		}
	}

	start := time.Now()
	_, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: cfg.Atespace, Name: actor},
		ActorTemplateNamespace: cfg.ActorTemplateNamespace,
		ActorTemplateName:      cfg.ActorTemplateName,
	}})
	end := time.Now()
	if err == nil {
		created = true
	}
	record("create", start, end, err)
	if err != nil {
		return failed
	}

	start = time.Now()
	_, err = client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref, Boot: true})
	end = time.Now()
	record("resume_boot", start, end, err)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	source, err = currentPlacement(ctx, client, ref)
	if err != nil {
		record("capture_source_worker", time.Now(), time.Now(), err)
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}
	if prober != nil {
		start = time.Now()
		beforeProbe, err = prober.Probe(ctx, cfg.Atespace, actor, cfg.ProbePath)
		end = time.Now()
		recordHTTPProbe("http_probe_before_suspend", start, end, beforeProbe, err)
		if err != nil {
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}

	start = time.Now()
	_, err = client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	end = time.Now()
	record("suspend_external", start, end, err)
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	if !cfg.CrossNodeResumeTargeting {
		blockers, err = drainSourceNode(ctx, client, enc, encMu, drainBlockerLifecycleConfig(cfg), actor, source)
		if err != nil {
			record("drain_source_node", time.Now(), time.Now(), err)
			cleanupBlockers(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, blockers)
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return true
		}
	}

	start = time.Now()
	resumeReq := &ateapipb.ResumeActorRequest{Actor: ref}
	if cfg.CrossNodeResumeTargeting {
		resumeReq.AvoidNodeName = source.node
	}
	_, err = client.ResumeActor(ctx, resumeReq)
	end = time.Now()
	if err == nil {
		target, err = currentPlacement(ctx, client, ref)
	}
	if err == nil && target.node == source.node {
		err = status.Errorf(codes.FailedPrecondition, "recover landed on same node %s", target.node)
	}
	record("cross_node_recover", start, end, err)
	if err != nil {
		cleanupBlockers(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, blockers)
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}
	if prober != nil {
		start = time.Now()
		var afterProbe httpProbeResult
		var probeErr error
		if cfg.PostRecoverProbeTimeout > 0 {
			afterProbe, probeErr = waitForHTTPProbe(ctx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.PostRecoverProbeTimeout)
		} else {
			afterProbe, probeErr = prober.Probe(ctx, cfg.Atespace, actor, cfg.ProbePath)
		}
		if probeErr == nil {
			probeErr = validateHTTPProbeContinuity(beforeProbe, afterProbe)
		}
		end = time.Now()
		recordHTTPProbe("http_probe_after_recover", start, end, afterProbe, probeErr)
		if probeErr != nil {
			cleanupBlockers(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, blockers)
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}

	start = time.Now()
	_, err = client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	end = time.Now()
	record("suspend_after_recover", start, end, err)
	if err != nil {
		cleanupBlockers(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, blockers)
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}

	start = time.Now()
	_, err = client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	end = time.Now()
	if err == nil || status.Code(err) == codes.NotFound {
		deleted = true
	}
	record("delete", start, end, err)
	cleanupBlockers(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, blockers)
	if created && !deleted {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
	}
	return failed
}

func drainBlockerLifecycleConfig(cfg CrossNodeRecoverConfig) LifecycleConfig {
	blockerCfg := cfg.LifecycleConfig
	if cfg.DrainBlockerActorTemplateNamespace != "" {
		blockerCfg.ActorTemplateNamespace = cfg.DrainBlockerActorTemplateNamespace
	}
	if cfg.DrainBlockerActorTemplateName != "" {
		blockerCfg.ActorTemplateName = cfg.DrainBlockerActorTemplateName
	}
	return blockerCfg
}

type workerPlacement struct {
	namespace    string
	pod          string
	pool         string
	node         string
	sandboxClass string
	labels       map[string]string
}

func (p workerPlacement) key() string {
	if p.namespace == "" && p.pod == "" {
		return ""
	}
	return p.namespace + "/" + p.pod
}

func currentPlacement(ctx context.Context, client *ateclient.Client, ref *ateapipb.ObjectRef) (workerPlacement, error) {
	actor, err := client.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
	if err != nil {
		return workerPlacement{}, fmt.Errorf("get actor placement: %w", err)
	}
	if actor.GetAteomPodNamespace() == "" || actor.GetAteomPodName() == "" {
		return workerPlacement{}, fmt.Errorf("actor has no active worker")
	}
	resp, err := client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		return workerPlacement{}, fmt.Errorf("list workers for placement: %w", err)
	}
	for _, w := range resp.GetWorkers() {
		if w.GetWorkerNamespace() == actor.GetAteomPodNamespace() && w.GetWorkerPod() == actor.GetAteomPodName() {
			return placementFromWorker(w), nil
		}
	}
	return workerPlacement{}, fmt.Errorf("active worker %s/%s not found in ListWorkers", actor.GetAteomPodNamespace(), actor.GetAteomPodName())
}

func placementFromWorker(w *ateapipb.Worker) workerPlacement {
	return workerPlacement{
		namespace:    w.GetWorkerNamespace(),
		pod:          w.GetWorkerPod(),
		pool:         w.GetWorkerPool(),
		node:         w.GetNodeName(),
		sandboxClass: w.GetSandboxClass(),
		labels:       maps.Clone(w.GetLabels()),
	}
}

func placementFromRouteTarget(target *ateapipb.RouteTarget) workerPlacement {
	if target == nil {
		return workerPlacement{}
	}
	return workerPlacement{
		namespace: target.GetAteomPodNamespace(),
		pod:       target.GetAteomPodName(),
		pool:      target.GetWorkerPoolName(),
		node:      target.GetNodeName(),
	}
}

func applyPlacementToEvent(ev *perfdata.LifecycleEvent, source, target workerPlacement) {
	if source.node != "" {
		ev.SourceWorker = source.key()
		ev.SourceNode = source.node
	}
	if target.node != "" {
		ev.TargetWorker = target.key()
		ev.TargetNode = target.node
		ev.CrossNode = source.node != "" && source.node != target.node
	}
}

func validateHotMigrationSummary(source, target workerPlacement, beforeGen, afterGen int64, beforeProbe, afterProbe httpProbeResult, requireCrossNode bool) error {
	if beforeProbe.StatusCode != http.StatusOK {
		return fmt.Errorf("before HTTP status=%d, want 200", beforeProbe.StatusCode)
	}
	if afterProbe.StatusCode != http.StatusOK {
		return fmt.Errorf("after HTTP status=%d, want 200", afterProbe.StatusCode)
	}
	if requireCrossNode && source.node != "" && target.node != "" && source.node == target.node {
		return status.Errorf(codes.FailedPrecondition, "source and target are on the same node %s", source.node)
	}
	if beforeGen > 0 && afterGen <= beforeGen {
		return status.Errorf(codes.FailedPrecondition, "route generation did not increase: before=%d after=%d", beforeGen, afterGen)
	}
	if beforeProbe.Checksum != "" && afterProbe.Checksum != "" && beforeProbe.Checksum != afterProbe.Checksum {
		return fmt.Errorf("checksum changed across hot migration: before=%s after=%s", beforeProbe.Checksum, afterProbe.Checksum)
	}
	if afterProbe.Counter < beforeProbe.Counter {
		return fmt.Errorf("counter regressed across hot migration: before=%d after=%d", beforeProbe.Counter, afterProbe.Counter)
	}
	return nil
}

func drainSourceNode(ctx context.Context, client *ateclient.Client, enc *json.Encoder, encMu *sync.Mutex, cfg LifecycleConfig, actor string, source workerPlacement) ([]string, error) {
	if source.node == "" {
		return nil, fmt.Errorf("source node is empty")
	}
	resp, err := client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
	if err != nil {
		return nil, fmt.Errorf("list workers before drain: %w", err)
	}
	if countFreeEligibleWorkers(resp.GetWorkers(), source, false) == 0 {
		return nil, fmt.Errorf("no free non-source workers available for cross-node recover")
	}

	var blockers []string
	maxBlockers := len(resp.GetWorkers())
	for attempt := 0; attempt < maxBlockers; attempt++ {
		resp, err = client.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
		if err != nil {
			return blockers, fmt.Errorf("list workers during drain: %w", err)
		}
		if countFreeEligibleWorkers(resp.GetWorkers(), source, true) == 0 {
			if countFreeEligibleWorkers(resp.GetWorkers(), source, false) == 0 {
				return blockers, fmt.Errorf("source node drained but no non-source free workers remain")
			}
			return blockers, nil
		}
		blocker := blockerName(actor, attempt)
		blockerRef := &ateapipb.ObjectRef{Atespace: cfg.Atespace, Name: blocker}
		blockers = append(blockers, blocker)
		if err := runBlockerCreateAndResume(ctx, client, enc, encMu, cfg, actor, blocker, blockerRef); err != nil {
			return blockers, err
		}
	}
	return blockers, fmt.Errorf("source node still has free eligible workers after %d blockers", maxBlockers)
}

func blockerName(parent string, attempt int) string {
	return heldActorName(parent, "-b", attempt)
}

func preBlockerName(parent string, attempt int) string {
	return heldActorName(parent, "-p", attempt)
}

func heldActorName(parent, marker string, attempt int) string {
	const maxActorNameLen = 63
	suffix := fmt.Sprintf("%s%02d", marker, attempt)
	limit := maxActorNameLen - len(suffix)
	if len(parent) > limit {
		parent = strings.TrimRight(parent[:limit], "-")
	}
	if parent == "" {
		parent = "blocker"
	}
	return parent + suffix
}

func runBlockerCreateAndResume(ctx context.Context, client *ateclient.Client, enc *json.Encoder, encMu *sync.Mutex, cfg LifecycleConfig, actor, blocker string, ref *ateapipb.ObjectRef) error {
	return runHeldActorCreateAndResume(ctx, client, enc, encMu, cfg, actor, blocker, ref, "blocker")
}

func runHeldActorCreateAndResume(ctx context.Context, client *ateclient.Client, enc *json.Encoder, encMu *sync.Mutex, cfg LifecycleConfig, actor, heldActor string, ref *ateapipb.ObjectRef, opPrefix string) error {
	ops := []struct {
		name string
		fn   func(context.Context) error
	}{
		{name: opPrefix + "_create", fn: func(ctx context.Context) error {
			_, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: cfg.Atespace, Name: heldActor},
				ActorTemplateNamespace: cfg.ActorTemplateNamespace,
				ActorTemplateName:      cfg.ActorTemplateName,
			}})
			return err
		}},
		{name: opPrefix + "_resume_boot", fn: func(ctx context.Context) error {
			_, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref, Boot: true})
			return err
		}},
	}
	for _, op := range ops {
		start := time.Now()
		err := op.fn(ctx)
		end := time.Now()
		ev := perfdata.LifecycleEvent{
			RunID:       cfg.RunID,
			OperationID: fmt.Sprintf("%s-%s-%s", actor, heldActor, op.name),
			ActorID:     cfg.Atespace + "/" + heldActor,
			Operation:   op.name,
			Stage:       cfg.Stage,
			StartTS:     start.UTC().Format(time.RFC3339Nano),
			EndTS:       end.UTC().Format(time.RFC3339Nano),
			DurationMS:  end.Sub(start).Milliseconds(),
			Workload:    cfg.Workload,
			Round:       cfg.Round,
			NodeScale:   cfg.NodeScale,
			Concurrency: cfg.Concurrency,
			Result:      "ok",
		}
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
		}
		encMu.Lock()
		_ = enc.Encode(ev)
		encMu.Unlock()
		if err != nil {
			return fmt.Errorf("%s %s: %w", op.name, heldActor, err)
		}
	}
	return nil
}

func cleanupBlockers(ctx context.Context, client *ateclient.Client, enc *json.Encoder, encMu *sync.Mutex, cfg LifecycleConfig, parent string, blockers []string) {
	for i := len(blockers) - 1; i >= 0; i-- {
		name := blockers[i]
		ref := &ateapipb.ObjectRef{Atespace: cfg.Atespace, Name: name}
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg, parent+"-"+name, ref)
	}
}

func countFreeEligibleWorkers(workers []*ateapipb.Worker, source workerPlacement, sourceNode bool) int {
	count := 0
	for _, w := range workers {
		if w.GetAssignment() != nil {
			continue
		}
		if sourceNode && w.GetNodeName() != source.node {
			continue
		}
		if !sourceNode && w.GetNodeName() == source.node {
			continue
		}
		if w.GetSandboxClass() != source.sandboxClass {
			continue
		}
		if !maps.Equal(w.GetLabels(), source.labels) {
			continue
		}
		count++
	}
	return count
}

func CleanupAtespace(ctx context.Context, kubeconfig, atespace string, concurrency int, out io.Writer) error {
	if kubeconfig == "" || atespace == "" {
		return fmt.Errorf("kubeconfig and atespace are required")
	}
	if concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	client, cleanup, err := newATEClient(ctx, kubeconfig)
	if err != nil {
		return fmt.Errorf("new ate client: %w", err)
	}
	defer client.Close()
	defer cleanup()

	enc := json.NewEncoder(out)
	var encMu sync.Mutex
	write := func(ev CleanupEvent) {
		encMu.Lock()
		defer encMu.Unlock()
		_ = enc.Encode(ev)
	}

	var actors []string
	pageToken := ""
	for {
		resp, err := client.ListActors(ctx, &ateapipb.ListActorsRequest{Atespace: atespace, PageSize: 500, PageToken: pageToken})
		if err != nil {
			return fmt.Errorf("list actors: %w", err)
		}
		for _, actor := range resp.GetActors() {
			actors = append(actors, actor.GetMetadata().GetName())
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for actor := range jobs {
				ref := &ateapipb.ObjectRef{Atespace: atespace, Name: actor}
				if _, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
					write(CleanupEvent{Atespace: atespace, Actor: actor, Action: "suspend", Result: "error", Code: status.Code(err).String(), Error: err.Error()})
				} else {
					write(CleanupEvent{Atespace: atespace, Actor: actor, Action: "suspend", Result: "ok"})
				}
				if _, err := client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
					code := status.Code(err)
					if code == codes.NotFound {
						write(CleanupEvent{Atespace: atespace, Actor: actor, Action: "delete", Result: "ok", Code: code.String()})
					} else {
						write(CleanupEvent{Atespace: atespace, Actor: actor, Action: "delete", Result: "error", Code: code.String(), Error: err.Error()})
					}
				} else {
					write(CleanupEvent{Atespace: atespace, Actor: actor, Action: "delete", Result: "ok"})
				}
			}
		}()
	}
	for _, actor := range actors {
		jobs <- actor
	}
	close(jobs)
	wg.Wait()

	resp, err := client.ListActors(ctx, &ateapipb.ListActorsRequest{Atespace: atespace, PageSize: 1})
	if err != nil {
		return fmt.Errorf("verify actors: %w", err)
	}
	if len(resp.GetActors()) > 0 {
		return fmt.Errorf("atespace %s still has actors after cleanup", atespace)
	}
	if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: atespace}}); err != nil {
		write(CleanupEvent{Atespace: atespace, Action: "delete_atespace", Result: "error", Code: status.Code(err).String(), Error: err.Error()})
		return fmt.Errorf("delete atespace: %w", err)
	}
	write(CleanupEvent{Atespace: atespace, Action: "delete_atespace", Result: "ok"})
	return nil
}

type httpProbeResult struct {
	StatusCode int
	Checksum   string
	Counter    int64
	InstanceID string
	NodeName   string
	Body       string
	Stale      bool
}

func parseHTTPProbeBody(body []byte) httpProbeResult {
	result := httpProbeResult{Body: strings.TrimSpace(string(body))}
	if len(result.Body) > 512 {
		result.Body = result.Body[:512]
	}
	var payload struct {
		Checksum   string `json:"checksum"`
		Counter    int64  `json:"counter"`
		InstanceID string `json:"instance_id"`
		NodeName   string `json:"node_name"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		result.Checksum = payload.Checksum
		result.Counter = payload.Counter
		result.InstanceID = payload.InstanceID
		result.NodeName = payload.NodeName
	}
	return result
}

func validateHTTPProbeContinuity(before, after httpProbeResult) error {
	if before.StatusCode != http.StatusOK {
		return fmt.Errorf("before HTTP status=%d, want 200", before.StatusCode)
	}
	if after.StatusCode != http.StatusOK {
		return fmt.Errorf("after HTTP status=%d, want 200", after.StatusCode)
	}
	if before.Checksum != "" && after.Checksum != "" && before.Checksum != after.Checksum {
		return fmt.Errorf("checksum changed across recover: before=%s after=%s", before.Checksum, after.Checksum)
	}
	return nil
}

type routerHTTPProber struct {
	baseURL string
	client  *http.Client
	stopCh  chan struct{}
}

func newRouterHTTPProber(ctx context.Context, kubeconfig string) (*routerHTTPProber, error) {
	if baseURL := routerHTTPBaseURLOverride(); baseURL != "" {
		return &routerHTTPProber{
			baseURL: strings.TrimRight(baseURL, "/"),
			client:  &http.Client{Timeout: 30 * time.Second},
		}, nil
	}

	config, err := ateclient.LoadConfig(kubeconfig, "")
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("new k8s client: %w", err)
	}
	const namespace = "ate-system"
	serviceName := routerHTTPServiceName()
	svc, err := clientset.CoreV1().Services(namespace).Get(ctx, serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s service: %w", serviceName, err)
	}
	if len(svc.Spec.Selector) == 0 {
		return nil, fmt.Errorf("service %s has no selector", serviceName)
	}
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String()})
	if err != nil {
		return nil, fmt.Errorf("list router pods: %w", err)
	}
	var targetPod *corev1.Pod
	for i := range pods.Items {
		if podReady(&pods.Items[i]) {
			targetPod = &pods.Items[i]
			break
		}
	}
	if targetPod == nil {
		return nil, fmt.Errorf("no ready %s pod in %s", serviceName, namespace)
	}
	targetPort, err := serviceHTTPPort(svc, targetPod)
	if err != nil {
		return nil, err
	}
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(targetPod.Name).
		SubResource("portforward")
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("create spdy transport: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())
	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	forwarder, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", targetPort)}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create port forwarder: %w", err)
	}
	errCh := make(chan error, 1)
	go func() {
		if err := forwarder.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()
	select {
	case <-readyCh:
	case err := <-errCh:
		close(stopCh)
		return nil, fmt.Errorf("port forward failed: %w", err)
	case <-ctx.Done():
		close(stopCh)
		return nil, ctx.Err()
	}
	ports, err := forwarder.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stopCh)
		return nil, fmt.Errorf("get forwarded port: %w", err)
	}
	return &routerHTTPProber{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", ports[0].Local),
		client:  &http.Client{Timeout: 30 * time.Second},
		stopCh:  stopCh,
	}, nil
}

func routerHTTPServiceName() string {
	if serviceName := os.Getenv("PERFKIT_ROUTER_SERVICE_NAME"); serviceName != "" {
		return serviceName
	}
	return "atenet-router"
}

func routerHTTPBaseURLOverride() string {
	return os.Getenv("PERFKIT_ROUTER_BASE_URL")
}

func (p *routerHTTPProber) Close() {
	if p.stopCh != nil {
		close(p.stopCh)
	}
}

func (p *routerHTTPProber) Probe(ctx context.Context, atespace, actor, path string) (httpProbeResult, error) {
	return p.Do(ctx, http.MethodGet, atespace, actor, path)
}

func (p *routerHTTPProber) Do(ctx context.Context, method, atespace, actor, path string) (httpProbeResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path, nil)
	if err != nil {
		return httpProbeResult{}, err
	}
	req.Method = method
	req.Host = resources.ActorDNSName(atespace, actor)
	resp, err := p.client.Do(req)
	if err != nil {
		return httpProbeResult{}, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	result := parseHTTPProbeBody(body)
	result.StatusCode = resp.StatusCode
	result.Stale = resp.Header.Get("X-Substrate-Stale") == "true"
	if readErr != nil {
		return result, readErr
	}
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("HTTP status=%d body=%q", resp.StatusCode, result.Body)
	}
	return result, nil
}

type continuousProbeStats struct {
	requests          int
	successes         int
	failures          int
	statusCounts      map[string]int
	longestSuccessGap time.Duration
	samples           []continuousProbeSample
}

type continuousProbeRun struct {
	done <-chan continuousProbeStats
}

type continuousProbeSample struct {
	sequence int
	start    time.Time
	end      time.Time
	result   httpProbeResult
	err      error
}

func startContinuousProbe(ctx context.Context, prober *routerHTTPProber, atespace, actor, path string, interval time.Duration) continuousProbeRun {
	return startContinuousProbeWithDo(ctx, func(ctx context.Context) (httpProbeResult, error) {
		return prober.Probe(ctx, atespace, actor, path)
	}, interval)
}

func startContinuousProbeWithDo(ctx context.Context, probe func(context.Context) (httpProbeResult, error), interval time.Duration) continuousProbeRun {
	done := make(chan continuousProbeStats, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			samples []continuousProbeSample
			nextSeq int
		)
		launch := func() {
			nextSeq++
			seq := nextSeq
			start := time.Now()
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := probe(ctx)
				sample := continuousProbeSample{
					sequence: seq,
					start:    start,
					end:      time.Now(),
					result:   result,
					err:      err,
				}
				mu.Lock()
				samples = append(samples, sample)
				mu.Unlock()
			}()
		}

		launch()
		for running := true; running; {
			select {
			case <-ctx.Done():
				running = false
			case <-ticker.C:
				launch()
			}
		}
		wg.Wait()

		mu.Lock()
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].sequence < samples[j].sequence
		})
		mu.Unlock()

		stats := continuousProbeStats{statusCounts: map[string]int{}, samples: samples}
		var lastSuccess time.Time
		for _, sample := range samples {
			stats.requests++
			statusKey := "error"
			if sample.result.StatusCode > 0 {
				statusKey = strconv.Itoa(sample.result.StatusCode)
			}
			stats.statusCounts[statusKey]++
			if sample.err == nil && sample.result.StatusCode == http.StatusOK {
				stats.successes++
				if !lastSuccess.IsZero() {
					if gap := sample.start.Sub(lastSuccess); gap > stats.longestSuccessGap {
						stats.longestSuccessGap = gap
					}
				}
				lastSuccess = sample.start
			} else {
				stats.failures++
			}
		}
		done <- stats
	}()
	return continuousProbeRun{done: done}
}

func continuousProbeSampleEvent(runID, stage, workload, round, actorID, actor string, sampleIndex, nodeScale int, sample continuousProbeSample) perfdata.LifecycleEvent {
	ev := perfdata.LifecycleEvent{
		RunID:          runID,
		OperationID:    fmt.Sprintf("%s-%03d-continuous-probe-%04d", actor, sampleIndex, sample.sequence),
		ActorID:        actorID,
		Operation:      "continuous_probe_sample",
		Stage:          stage,
		StartTS:        sample.start.UTC().Format(time.RFC3339Nano),
		EndTS:          sample.end.UTC().Format(time.RFC3339Nano),
		DurationMS:     sample.end.Sub(sample.start).Milliseconds(),
		Workload:       workload,
		Round:          round,
		NodeScale:      nodeScale,
		Concurrency:    1,
		Result:         "ok",
		HTTPStatus:     sample.result.StatusCode,
		HTTPChecksum:   sample.result.Checksum,
		HTTPCounter:    sample.result.Counter,
		HTTPInstanceID: sample.result.InstanceID,
		HTTPNodeName:   sample.result.NodeName,
		HTTPBody:       sample.result.Body,
		HTTPStale:      sample.result.Stale,
		ProbeSequence:  sample.sequence,
	}
	if sample.err != nil {
		ev.Result = "error"
		ev.ErrorCode = status.Code(sample.err).String()
		ev.ErrorReason = sample.err.Error()
	}
	return ev
}

func waitForHTTPProbe(ctx context.Context, prober *routerHTTPProber, atespace, actor, path string, timeout time.Duration) (httpProbeResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		result, err := prober.Probe(waitCtx, atespace, actor, path)
		if err == nil && result.StatusCode == http.StatusOK {
			return result, nil
		}
		if err != nil {
			lastErr = err
		}
		select {
		case <-waitCtx.Done():
			if lastErr != nil {
				return result, fmt.Errorf("timeout waiting for HTTP probe after migration: %w; last error: %v", waitCtx.Err(), lastErr)
			}
			return result, fmt.Errorf("timeout waiting for HTTP probe after migration: %w", waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func serviceHTTPPort(svc *corev1.Service, pod *corev1.Pod) (int32, error) {
	for _, port := range svc.Spec.Ports {
		if port.Port != 80 {
			continue
		}
		switch port.TargetPort.Type {
		case intstr.Int:
			if port.TargetPort.IntVal > 0 {
				return port.TargetPort.IntVal, nil
			}
		case intstr.String:
			for _, c := range pod.Spec.Containers {
				for _, cp := range c.Ports {
					if cp.Name == port.TargetPort.StrVal && cp.ContainerPort > 0 {
						return cp.ContainerPort, nil
					}
				}
			}
			return 0, fmt.Errorf("targetPort %q not found on router pod %s", port.TargetPort.StrVal, pod.Name)
		}
	}
	return 0, fmt.Errorf("service %s has no usable port 80", svc.Name)
}

func newATEClient(ctx context.Context, kubeconfig string) (*ateclient.Client, func(), error) {
	client, err := ateclient.NewClient(ctx, kubeconfig, "", "", false)
	if err == nil {
		return client, func() {}, nil
	}
	fallbackClient, cleanup, fallbackErr := newKubectlPortForwardClient(ctx, kubeconfig)
	if fallbackErr != nil {
		return nil, func() {}, fmt.Errorf("%w; kubectl port-forward fallback failed: %v", err, fallbackErr)
	}
	return fallbackClient, cleanup, nil
}

func waitForActorTemplateGolden(ctx context.Context, kubeconfig, namespace, name string, timeout time.Duration) error {
	config, err := ateclient.LoadConfig(kubeconfig, "")
	if err != nil {
		return fmt.Errorf("load kubeconfig for ActorTemplate readiness: %w", err)
	}
	client, err := substrateclientset.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create substrate client for ActorTemplate readiness: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var lastPhase atev1alpha1.PhaseType
	var lastGolden string
	for {
		at, err := client.ApiV1alpha1().ActorTemplates(namespace).Get(waitCtx, name, metav1.GetOptions{})
		if err == nil {
			lastPhase = at.Status.Phase
			lastGolden = at.Status.GoldenSnapshot
			if at.Status.Phase == atev1alpha1.PhaseReady && at.Status.GoldenSnapshot != "" {
				return nil
			}
			if at.Status.Phase == atev1alpha1.PhaseFailed {
				return fmt.Errorf("ActorTemplate %s/%s is Failed before hot migration; golden snapshot=%q", namespace, name, at.Status.GoldenSnapshot)
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timeout waiting for ActorTemplate %s/%s Ready with golden snapshot: last phase=%s golden=%q: %w", namespace, name, lastPhase, lastGolden, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func newKubectlPortForwardClient(ctx context.Context, kubeconfig string) (*ateclient.Client, func(), error) {
	token, err := kubectlToken(ctx, kubeconfig)
	if err != nil {
		return nil, func() {}, err
	}

	pfCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(pfCtx, "kubectl", "--kubeconfig", kubeconfig, "-n", "ate-system", "port-forward", "svc/api", ":443")
	pipeReader, pipeWriter := io.Pipe()
	cmd.Stdout = pipeWriter
	cmd.Stderr = pipeWriter
	if err := cmd.Start(); err != nil {
		cancel()
		_ = pipeWriter.Close()
		return nil, func() {}, fmt.Errorf("start kubectl port-forward: %w", err)
	}

	cleanup := func() {
		cancel()
		_ = pipeWriter.Close()
		_ = pipeReader.Close()
		_ = cmd.Wait()
	}

	portCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		defer close(portCh)
		scanner := bufio.NewScanner(pipeReader)
		portPattern := regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+)`)
		for scanner.Scan() {
			if match := portPattern.FindStringSubmatch(scanner.Text()); len(match) == 2 {
				portCh <- match[1]
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errCh <- err
			return
		}
		errCh <- fmt.Errorf("kubectl port-forward exited before reporting a local port")
	}()

	var port string
	select {
	case port = <-portCh:
		if port == "" {
			cleanup()
			return nil, func() {}, fmt.Errorf("kubectl port-forward did not report a local port")
		}
	case err := <-errCh:
		cleanup()
		return nil, func() {}, err
	case <-time.After(15 * time.Second):
		cleanup()
		return nil, func() {}, fmt.Errorf("timed out waiting for kubectl port-forward readiness")
	case <-ctx.Done():
		cleanup()
		return nil, func() {}, ctx.Err()
	}

	client, err := ateclient.NewClientWithBearerToken(ctx, "127.0.0.1:"+port, token, false)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return client, cleanup, nil
}

func kubectlToken(ctx context.Context, kubeconfig string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "-n", "ate-system", "create", "token", "ate-client", "--audience=api.ate-system.svc", "--duration=1h")
	data, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return "", fmt.Errorf("kubectl create token: %w: %s", err, strings.TrimSpace(string(ee.Stderr)))
		}
		return "", fmt.Errorf("kubectl create token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("kubectl create token returned an empty token")
	}
	return token, nil
}

func runActorLifecycle(ctx context.Context, client *ateclient.Client, enc *json.Encoder, encMu *sync.Mutex, cfg LifecycleConfig, i int) bool {
	actor := fmt.Sprintf("%s-%03d-%d", cfg.ActorPrefix, i, time.Now().UnixNano())
	ref := &ateapipb.ObjectRef{Atespace: cfg.Atespace, Name: actor}
	failed := false
	created := false
	deleted := false
	type lifecycleOp struct {
		name string
		fn   func(context.Context) error
	}
	ops := []lifecycleOp{
		{name: "create", fn: func(ctx context.Context) error {
			_, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: cfg.Atespace, Name: actor},
				ActorTemplateNamespace: cfg.ActorTemplateNamespace,
				ActorTemplateName:      cfg.ActorTemplateName,
			}})
			if err == nil {
				created = true
			}
			return err
		}},
		{name: "resume_boot", fn: func(ctx context.Context) error {
			_, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref, Boot: true})
			return err
		}},
		{name: "suspend_1", fn: func(ctx context.Context) error {
			_, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
			return err
		}},
		{name: "resume_warm", fn: func(ctx context.Context) error {
			_, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref})
			return err
		}},
	}
	if cfg.PostWarmResumeSleep > 0 {
		ops = append(ops, lifecycleOp{name: "post_warm_resume_sleep", fn: func(ctx context.Context) error {
			timer := time.NewTimer(cfg.PostWarmResumeSleep)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}})
	}
	ops = append(ops,
		lifecycleOp{name: "suspend_2", fn: func(ctx context.Context) error {
			_, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
			return err
		}},
		lifecycleOp{name: "delete", fn: func(ctx context.Context) error {
			_, err := client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
			if err == nil || status.Code(err) == codes.NotFound {
				deleted = true
			}
			return err
		}},
	)
	for _, op := range ops {
		start := time.Now()
		err := op.fn(ctx)
		end := time.Now()
		ev := perfdata.LifecycleEvent{
			RunID:       cfg.RunID,
			OperationID: fmt.Sprintf("%s-%03d-%s", actor, i, op.name),
			ActorID:     cfg.Atespace + "/" + actor,
			Operation:   op.name,
			Stage:       cfg.Stage,
			StartTS:     start.UTC().Format(time.RFC3339Nano),
			EndTS:       end.UTC().Format(time.RFC3339Nano),
			DurationMS:  end.Sub(start).Milliseconds(),
			Workload:    cfg.Workload,
			Round:       cfg.Round,
			NodeScale:   cfg.NodeScale,
			Concurrency: cfg.Concurrency,
			Result:      "ok",
		}
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
			failed = true
		}
		encMu.Lock()
		_ = enc.Encode(ev)
		encMu.Unlock()
		if err != nil {
			if created && !deleted {
				runBestEffortActorCleanup(ctx, client, enc, encMu, cfg, actor, ref)
			}
			break
		}
		if cfg.AsyncSuspend && (op.name == "suspend_1" || op.name == "suspend_2") {
			waitStart := time.Now()
			waitErr := waitForActorStatus(ctx, client, ref, ateapipb.Actor_STATUS_SUSPENDED, cfg.CheckpointReadyTimeout)
			waitEnd := time.Now()
			waitEv := perfdata.LifecycleEvent{
				RunID:       cfg.RunID,
				OperationID: fmt.Sprintf("%s-%03d-%s-checkpoint-ready", actor, i, op.name),
				ActorID:     cfg.Atespace + "/" + actor,
				Operation:   op.name + "_checkpoint_ready",
				Stage:       cfg.Stage,
				StartTS:     waitStart.UTC().Format(time.RFC3339Nano),
				EndTS:       waitEnd.UTC().Format(time.RFC3339Nano),
				DurationMS:  waitEnd.Sub(waitStart).Milliseconds(),
				Workload:    cfg.Workload,
				Round:       cfg.Round,
				NodeScale:   cfg.NodeScale,
				Concurrency: cfg.Concurrency,
				Result:      "ok",
			}
			if waitErr != nil {
				waitEv.Result = "error"
				waitEv.ErrorCode = status.Code(waitErr).String()
				waitEv.ErrorReason = waitErr.Error()
				failed = true
			}
			encMu.Lock()
			_ = enc.Encode(waitEv)
			encMu.Unlock()
			if waitErr != nil {
				if created && !deleted {
					runBestEffortActorCleanup(ctx, client, enc, encMu, cfg, actor, ref)
				}
				break
			}
		}
	}
	return failed
}

func waitForActorStatus(ctx context.Context, client *ateclient.Client, ref *ateapipb.ObjectRef, want ateapipb.Actor_Status, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		actor, err := client.GetActor(deadlineCtx, &ateapipb.GetActorRequest{Actor: ref})
		if err != nil {
			return fmt.Errorf("get actor while waiting for %s: %w", want, err)
		}
		if actor.GetStatus() == want {
			return nil
		}
		if actor.GetStatus() == ateapipb.Actor_STATUS_CRASHED {
			return fmt.Errorf("actor reached CRASHED while waiting for %s", want)
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("timeout waiting for actor status %s: last status %s: %w", want, actor.GetStatus(), deadlineCtx.Err())
		case <-ticker.C:
		}
	}
}

func runBestEffortActorCleanup(ctx context.Context, client *ateclient.Client, enc *json.Encoder, encMu *sync.Mutex, cfg LifecycleConfig, actor string, ref *ateapipb.ObjectRef) {
	ops := []struct {
		name string
		fn   func(context.Context) error
	}{
		{name: "cleanup_suspend", fn: func(ctx context.Context) error {
			_, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
			return err
		}},
		{name: "cleanup_delete", fn: func(ctx context.Context) error {
			_, err := client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
			if status.Code(err) == codes.NotFound {
				return nil
			}
			return err
		}},
	}
	for _, op := range ops {
		start := time.Now()
		err := op.fn(ctx)
		end := time.Now()
		ev := perfdata.LifecycleEvent{
			RunID:       cfg.RunID,
			OperationID: fmt.Sprintf("%s-cleanup-%s", actor, op.name),
			ActorID:     cfg.Atespace + "/" + actor,
			Operation:   op.name,
			Stage:       cfg.Stage,
			StartTS:     start.UTC().Format(time.RFC3339Nano),
			EndTS:       end.UTC().Format(time.RFC3339Nano),
			DurationMS:  end.Sub(start).Milliseconds(),
			Workload:    cfg.Workload,
			Round:       cfg.Round,
			NodeScale:   cfg.NodeScale,
			Concurrency: cfg.Concurrency,
			Result:      "ok",
		}
		if err != nil {
			ev.Result = "error"
			ev.ErrorCode = status.Code(err).String()
			ev.ErrorReason = err.Error()
		}
		encMu.Lock()
		_ = enc.Encode(ev)
		encMu.Unlock()
		if op.name == "cleanup_delete" || err != nil {
			return
		}
	}
}

func sanitizeName(v string) string {
	v = strings.ToLower(v)
	var b strings.Builder
	lastDash := false
	for _, r := range v {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}
