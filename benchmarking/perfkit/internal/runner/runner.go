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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
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
	"github.com/gorilla/websocket"
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
	ProbePath                 string
	ExtraProbePaths           []string
	SSEPath                   string
	TerminalWebSocketPath     string
	MutationPath              string
	PreMigrationPostPaths     []string
	StateMutations            int
	MutationInterval          time.Duration
	MutationBodyBytes         int64
	ProbeInterval             time.Duration
	ProbeReadLimitBytes       int64
	DrainTimeout              time.Duration
	PostCommitProbeTimeout    time.Duration
	CleanupIsolationDuration  time.Duration
	MigrationRPCTimeout       time.Duration
	LiveMigrationDowntimeMS   int64
	LiveMigrationConnections  int
	LiveMigrationMemoryMode   string
	RequireRouteSwitchedProbe bool
	SkipFinalActorCleanup     bool
	ActorTemplateTimeout      time.Duration
	TargetWorkerLabels        map[string]string
	RequireCrossNode          bool
	RequireQuiesce            bool
	BootSource                bool
}

type extraProbeConfig struct {
	Label string
	Path  string
}

type preMigrationPostConfig struct {
	Label string
	Path  string
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
	if _, err := parseExtraProbeConfigs(cfg.ExtraProbePaths); err != nil {
		return err
	}
	if _, err := parsePreMigrationPostConfigs(cfg.PreMigrationPostPaths); err != nil {
		return err
	}
	if cfg.MutationPath == "" {
		cfg.MutationPath = "/increment"
	}
	if cfg.ProbeReadLimitBytes <= 0 {
		cfg.ProbeReadLimitBytes = 64 * 1024
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
	if cfg.MigrationRPCTimeout <= 0 {
		cfg.MigrationRPCTimeout = 180 * time.Second
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
	prober.readLimitBytes = cfg.ProbeReadLimitBytes
	prober.client.Timeout = 6 * time.Second
	if cfg.PostCommitProbeTimeout > prober.client.Timeout {
		prober.client.Timeout = cfg.PostCommitProbeTimeout
	}

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

	if failures > 0 {
		return fmt.Errorf("hot migration completed with %d failed samples", failures)
	}
	if _, err := client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Atespace: &ateapipb.ObjectRef{Name: base.Atespace}}); err != nil {
		if cfg.SkipFinalActorCleanup {
			return nil
		}
		return fmt.Errorf("delete atespace after hot migration run: %w", err)
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
	cleanupStartedAt := time.Time{}
	cleanupEndedAt := time.Time{}
	var cleanupErr error

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
			RunID:                 cfg.RunID,
			OperationID:           fmt.Sprintf("%s-%03d-%s", actor, i, op),
			ActorID:               cfg.Atespace + "/" + actor,
			Operation:             op,
			Stage:                 cfg.Stage,
			StartTS:               start.UTC().Format(time.RFC3339Nano),
			EndTS:                 end.UTC().Format(time.RFC3339Nano),
			DurationMS:            end.Sub(start).Milliseconds(),
			Workload:              cfg.Workload,
			Round:                 cfg.Round,
			NodeScale:             cfg.NodeScale,
			Concurrency:           cfg.Concurrency,
			Result:                "ok",
			HTTPStatus:            result.StatusCode,
			HTTPChecksum:          result.Checksum,
			HTTPBodyBytes:         result.BodyBytes,
			HTTPIdempotencyKey:    result.IdempotencyKey,
			HTTPCounter:           result.Counter,
			HTTPStateVersion:      result.StateVersion,
			HTTPLastMutationID:    result.LastMutationID,
			HTTPInstanceID:        result.InstanceID,
			HTTPBootID:            result.BootID,
			HTTPPodName:           result.PodName,
			HTTPNodeName:          result.NodeName,
			HTTPRouteGeneration:   result.RouteGeneration,
			HTTPRoutePhase:        result.RoutePhase,
			HTTPRouteTargetWorker: result.RouteTargetWorker,
			HTTPRouteTargetNode:   result.RouteTargetNode,
			HTTPRouteTargetIP:     result.RouteTargetIP,
			HTTPBody:              result.Body,
			HTTPStale:             result.Stale,
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
			if sample.canceledByStop {
				continue
			}
			write(continuousProbeSampleEvent(cfg.RunID, cfg.Stage, cfg.Workload, cfg.Round, cfg.Atespace+"/"+actor, actor, i, cfg.NodeScale, sample))
		}
	}
	recordContinuousMutationSamples := func(samples []continuousProbeSample) {
		for _, sample := range samples {
			if sample.canceledByStop {
				continue
			}
			write(continuousMutationSampleEvent(cfg.RunID, cfg.Stage, cfg.Workload, cfg.Round, cfg.Atespace+"/"+actor, actor, i, cfg.NodeScale, sample))
		}
	}
	recordContinuousExtraProbeSamples := func(extra extraProbeConfig, samples []continuousProbeSample) {
		for _, sample := range samples {
			if sample.canceledByStop {
				continue
			}
			write(continuousExtraProbeSampleEvent(cfg.RunID, cfg.Stage, cfg.Workload, cfg.Round, cfg.Atespace+"/"+actor, actor, i, cfg.NodeScale, extra, sample))
		}
	}
	var stopProbes context.CancelFunc
	var probes continuousProbeRun
	var extraProbes []extraProbeRun
	var mutations continuousProbeRun
	var sse sseProbeRun
	var sseStartedBeforePrepare bool
	var terminal terminalWebSocketProbeRun
	var terminalStartedBeforePrepare bool
	recordFailedSummary := func(end time.Time, stats continuousProbeStats, extraStats []extraProbeStats, mutationStats continuousProbeStats, sseStats sseProbeStats, terminalStats terminalWebSocketStats, summaryErr error, stageDurationMS map[string]int64) {
		if summaryErr == nil {
			summaryErr = status.Error(codes.Unknown, "hot migration failed before final validation")
		}
		if cfg.SSEPath != "" {
			write(sseProbeSummaryEvent(cfg, actor, i, migrationStart, end, sseStats))
		}
		if cfg.TerminalWebSocketPath != "" {
			write(terminalWebSocketSummaryEvent(cfg, actor, i, migrationStart, end, terminalStats))
		}
		recordContinuousProbeSamples(stats.samples)
		for _, extra := range extraStats {
			recordContinuousExtraProbeSamples(extra.config, extra.stats.samples)
			extraErr := validateExtraProbeStats(extra.config, extra.stats, beforeProbe)
			write(continuousExtraProbeSummaryEvent(cfg.RunID, cfg.Stage, cfg.Workload, cfg.Round, cfg.Atespace+"/"+actor, actor, i, cfg.NodeScale, extra.config, extra.stats, extraErr))
		}
		if cfg.MutationInterval > 0 {
			recordContinuousMutationSamples(mutationStats.samples)
		}
		summary := failedHotMigrationSummaryEvent(cfg, actor, i, migrationStart, end, source, target, routeGenerationBefore, routeGenerationAfter, beforeProbe, stats, mutationStats, sseStats, terminalStats, stageDurationMS, summaryErr)
		write(summary)
	}
	stopAndRecordFailedSummary := func(summaryErr error, stageDurationMS map[string]int64) {
		stopProbes()
		stats := <-probes.done
		extraStats := drainExtraProbeRuns(extraProbes)
		var mutationStats continuousProbeStats
		if mutations.done != nil {
			mutationStats = <-mutations.done
		}
		var sseStats sseProbeStats
		if sse.done != nil {
			sseStats = <-sse.done
			sseStats.startedBeforePrepare = sseStartedBeforePrepare
			if sseStartedBeforePrepare {
				sseStats.eventsBeforePrepare = 1
			}
		}
		var terminalStats terminalWebSocketStats
		if terminal.done != nil {
			terminalStats = <-terminal.done
			terminalStats.startedBeforePrepare = terminalStartedBeforePrepare
		}
		recordFailedSummary(time.Now(), stats, extraStats, mutationStats, sseStats, terminalStats, summaryErr, stageDurationMS)
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

	mutationBodyBytes := mutationBody(cfg.MutationBodyBytes)
	for mutation := 0; mutation < cfg.StateMutations; mutation++ {
		start = time.Now()
		mutationID := fmt.Sprintf("%s-%03d-pre-mutation-%03d", actor, i, mutation)
		mutationProbe, mutateErr := prober.DoWithHeadersAndBody(ctx, http.MethodPost, cfg.Atespace, actor, cfg.MutationPath, map[string]string{"Idempotency-Key": mutationID}, mutationBodyBytes)
		mutationProbe.IdempotencyKey = mutationID
		end = time.Now()
		recordHTTPProbe(fmt.Sprintf("http_mutation_before_migration_%03d", mutation), start, end, mutationProbe, mutateErr)
		if mutateErr != nil {
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}
	preMigrationPosts, err := parsePreMigrationPostConfigs(cfg.PreMigrationPostPaths)
	if err != nil {
		record("parse_pre_migration_post_paths", time.Now(), time.Now(), nil, err)
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}
	for idx, post := range preMigrationPosts {
		start = time.Now()
		postID := fmt.Sprintf("%s-%03d-pre-post-%s-%03d", actor, i, post.Label, idx)
		postProbe, postErr := prober.DoWithHeadersAndBody(ctx, http.MethodPost, cfg.Atespace, actor, post.Path, map[string]string{"Idempotency-Key": postID}, nil)
		postProbe.IdempotencyKey = postID
		end = time.Now()
		recordHTTPProbe(fmt.Sprintf("http_pre_migration_post_%s_%03d", post.Label, idx), start, end, postProbe, postErr)
		if postErr != nil {
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

	probeCtx, cancelProbes := context.WithCancel(ctx)
	stopProbes = cancelProbes
	probes = startContinuousProbe(probeCtx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.ProbeInterval)
	extraProbeConfigs, err := parseExtraProbeConfigs(cfg.ExtraProbePaths)
	if err != nil {
		record("parse_extra_probe_paths", time.Now(), time.Now(), nil, err)
		stopProbes()
		<-probes.done
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}
	extraProbes = startExtraProbeRuns(probeCtx, prober, cfg.Atespace, actor, extraProbeConfigs, cfg.ProbeInterval)
	if cfg.MutationInterval > 0 {
		mutations = startContinuousMutationWithBody(probeCtx, prober, cfg.Atespace, actor, cfg.MutationPath, cfg.MutationInterval, mutationBodyBytes)
	}
	if cfg.SSEPath != "" {
		sse = startSSEProbe(probeCtx, prober, cfg.Atespace, actor, cfg.SSEPath)
		select {
		case readyErr := <-sse.ready:
			if readyErr != nil {
				err = fmt.Errorf("SSE did not produce a pre-migration event: %w", readyErr)
				record("sse_probe_prewarm", time.Now(), time.Now(), nil, err)
				stopProbes()
				<-probes.done
				drainExtraProbeRuns(extraProbes)
				if mutations.done != nil {
					<-mutations.done
				}
				if sse.done != nil {
					<-sse.done
				}
				runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
				return failed
			}
			sseStartedBeforePrepare = true
		case <-time.After(cfg.PostCommitProbeTimeout):
			err = fmt.Errorf("SSE did not produce a pre-migration event within %s", cfg.PostCommitProbeTimeout)
			record("sse_probe_prewarm", time.Now(), time.Now(), nil, err)
			stopProbes()
			<-probes.done
			drainExtraProbeRuns(extraProbes)
			if mutations.done != nil {
				<-mutations.done
			}
			if sse.done != nil {
				<-sse.done
			}
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}
	if cfg.TerminalWebSocketPath != "" {
		terminal = startTerminalWebSocketProbe(probeCtx, prober, cfg.Atespace, actor, cfg.TerminalWebSocketPath, cfg.ProbeInterval)
		select {
		case readyErr := <-terminal.ready:
			if readyErr != nil {
				err = fmt.Errorf("terminal websocket did not produce a pre-migration message: %w", readyErr)
				record("terminal_websocket_prewarm", time.Now(), time.Now(), nil, err)
				stopProbes()
				<-probes.done
				drainExtraProbeRuns(extraProbes)
				if mutations.done != nil {
					<-mutations.done
				}
				if sse.done != nil {
					<-sse.done
				}
				if terminal.done != nil {
					<-terminal.done
				}
				runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
				return failed
			}
			terminalStartedBeforePrepare = true
		case <-time.After(cfg.PostCommitProbeTimeout):
			err = fmt.Errorf("terminal websocket did not produce a pre-migration message within %s", cfg.PostCommitProbeTimeout)
			record("terminal_websocket_prewarm", time.Now(), time.Now(), nil, err)
			stopProbes()
			<-probes.done
			drainExtraProbeRuns(extraProbes)
			if mutations.done != nil {
				<-mutations.done
			}
			if sse.done != nil {
				<-sse.done
			}
			if terminal.done != nil {
				<-terminal.done
			}
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}
	migrationStart = time.Now()
	prepareCtx, cancelPrepare := context.WithTimeout(ctx, cfg.MigrationRPCTimeout)
	prepareResp, err := client.PrepareActorMigration(prepareCtx, &ateapipb.PrepareActorMigrationRequest{
		Actor:                ref,
		TargetWorkerSelector: &ateapipb.Selector{MatchLabels: cfg.TargetWorkerLabels},
		RequireCrossNode:     cfg.RequireCrossNode,
	})
	cancelPrepare()
	prepareEnd := time.Now()
	if err == nil {
		target = placementFromRouteTarget(prepareResp.GetActor().GetRoute().GetCandidate())
		prepareStageDurationMS = cloneStageDurationMS(prepareResp.GetActor().GetMigration().GetStageDurationMs())
	}
	record("prepare_actor_migration", migrationStart, prepareEnd, prepareStageDurationMS, err)
	if err != nil {
		stopAndRecordFailedSummary(err, prepareStageDurationMS)
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}

	commitStart := time.Now()
	commitCtx, cancelCommit := context.WithTimeout(ctx, cfg.MigrationRPCTimeout)
	commitResp, err := client.CommitActorMigration(commitCtx, &ateapipb.CommitActorMigrationRequest{
		Actor:          ref,
		DrainTimeoutMs: int32(cfg.DrainTimeout.Milliseconds()),
		RequireQuiesce: cfg.RequireQuiesce,
	})
	cancelCommit()
	commitEnd = time.Now()
	if err == nil {
		target = placementFromRouteTarget(commitResp.GetActor().GetRoute().GetActive())
		routeGenerationAfter = commitResp.GetActor().GetRoute().GetGeneration()
		commitStageDurationMS = cloneStageDurationMS(commitResp.GetActor().GetMigration().GetStageDurationMs())
	}
	record("commit_actor_migration", commitStart, commitEnd, commitStageDurationMS, err)
	if err != nil {
		stopAndRecordFailedSummary(err, mergeStageDurationMS(prepareStageDurationMS, commitStageDurationMS))
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
		drainExtraProbeRuns(extraProbes)
		if mutations.done != nil {
			<-mutations.done
		}
		if sse.done != nil {
			<-sse.done
		}
		if terminal.done != nil {
			<-terminal.done
		}
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return true
	}

	afterProbe, err = waitForHTTPProbe(ctx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.PostCommitProbeTimeout)
	afterEnd := time.Now()
	observationEndTime := afterEnd
	recordHTTPProbe("http_probe_after_migration", commitEnd, afterEnd, afterProbe, err)
	if err == nil && cfg.CleanupIsolationDuration > 0 {
		start = time.Now()
		waitErr := waitForCleanupIsolationWindow(ctx, cfg.CleanupIsolationDuration)
		end = time.Now()
		observationEndTime = observationEnd(afterEnd, end)
		record("cleanup_isolation_observation", start, end, nil, waitErr)
		if waitErr != nil {
			stopProbes()
			<-probes.done
			drainExtraProbeRuns(extraProbes)
			if mutations.done != nil {
				<-mutations.done
			}
			if sse.done != nil {
				<-sse.done
			}
			if terminal.done != nil {
				<-terminal.done
			}
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}
	stopProbes()
	stats := <-probes.done
	cleanupIsolationStats := postSwitchIsolationStats(stats, afterEnd)
	if !cleanupStartedAt.IsZero() {
		cleanupIsolationStats = probeStatsInWindow(stats, cleanupStartedAt, cleanupEndedAt)
	}
	routeSwitchedStats := routeSwitchedIsolationStats(stats, target)
	extraStats := drainExtraProbeRuns(extraProbes)
	var mutationStats continuousProbeStats
	if mutations.done != nil {
		mutationStats = <-mutations.done
	}
	mutationRouteSwitchedStats := routeSwitchedIsolationStats(mutationStats, target)
	var sseStats sseProbeStats
	if sse.done != nil {
		sseStats = <-sse.done
		sseStats.startedBeforePrepare = sseStartedBeforePrepare
		if sseStartedBeforePrepare {
			sseStats.eventsBeforePrepare = 1
		}
		write(sseProbeSummaryEvent(cfg, actor, i, migrationStart, observationEndTime, sseStats))
	}
	var terminalStats terminalWebSocketStats
	if terminal.done != nil {
		terminalStats = <-terminal.done
		terminalStats.startedBeforePrepare = terminalStartedBeforePrepare
		write(terminalWebSocketSummaryEvent(cfg, actor, i, migrationStart, observationEndTime, terminalStats))
	}
	recordContinuousProbeSamples(stats.samples)
	for _, extra := range extraStats {
		recordContinuousExtraProbeSamples(extra.config, extra.stats.samples)
	}
	if cfg.MutationInterval > 0 {
		recordContinuousMutationSamples(mutationStats.samples)
	}
	if err != nil {
		runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
		return failed
	}
	consistencyProbe := afterProbe
	if cfg.MutationInterval > 0 {
		start = time.Now()
		consistencyProbe, err = waitForHTTPProbe(ctx, prober, cfg.Atespace, actor, cfg.ProbePath, cfg.PostCommitProbeTimeout)
		end = time.Now()
		recordHTTPProbe("http_probe_after_continuous_mutations", start, end, consistencyProbe, err)
		if err != nil {
			write(failedHotMigrationSummaryEvent(cfg, actor, i, migrationStart, end, source, target, routeGenerationBefore, routeGenerationAfter, beforeProbe, stats, mutationStats, sseStats, terminalStats, mergeStageDurationMS(prepareStageDurationMS, commitStageDurationMS), err))
			runBestEffortActorCleanup(ctx, client, enc, encMu, cfg.LifecycleConfig, actor, ref)
			return failed
		}
	}
	summaryProbe := afterProbe
	if cfg.MutationInterval > 0 {
		summaryProbe = consistencyProbe
	}

	staleVersionCount := countStaleProbeVersions(stats.samples, beforeProbe)
	dualActive := dualActiveObserved(stats.samples)
	summaryErr := validateHotMigrationSummary(source, target, routeGenerationBefore, routeGenerationAfter, beforeProbe, afterProbe, cfg.RequireCrossNode)
	if continuityErr := validateContinuousProbeStats(stats, staleVersionCount, dualActive); continuityErr != nil {
		failed = true
		if summaryErr == nil {
			summaryErr = continuityErr
		}
	}
	for _, extra := range extraStats {
		extraErr := validateExtraProbeStats(extra.config, extra.stats, beforeProbe)
		write(continuousExtraProbeSummaryEvent(cfg.RunID, cfg.Stage, cfg.Workload, cfg.Round, cfg.Atespace+"/"+actor, actor, i, cfg.NodeScale, extra.config, extra.stats, extraErr))
		if extraErr != nil {
			failed = true
			if summaryErr == nil {
				summaryErr = extraErr
			}
		}
	}
	mutationDuplicateCount := countDuplicateSuccessCounters(mutationStats.samples)
	mutationMaxCounter := maxSuccessCounter(mutationStats.samples)
	mutationLostCount := countLostAcceptedMutations(mutationStats.samples, consistencyProbe)
	if cfg.MutationInterval > 0 {
		if !hasStateProof(consistencyProbe) {
			failed = true
			if summaryErr == nil {
				summaryErr = status.Errorf(codes.FailedPrecondition, "missing final continuous mutation state proof: counter=%d state_version=%d last_mutation_id=%q", consistencyProbe.Counter, consistencyProbe.StateVersion, consistencyProbe.LastMutationID)
			}
		}
		if mutationErr := validateContinuousMutationStats(mutationStats, mutationDuplicateCount, mutationLostCount); mutationErr != nil {
			failed = true
			if summaryErr == nil {
				summaryErr = mutationErr
			}
		}
	}
	if cfg.SSEPath != "" {
		if sseErr := validateSSEProbeStats(sseStats); sseErr != nil {
			failed = true
			if summaryErr == nil {
				summaryErr = sseErr
			}
		}
	}
	if cfg.TerminalWebSocketPath != "" {
		if terminalErr := validateTerminalWebSocketStats(terminalStats); terminalErr != nil {
			failed = true
			if summaryErr == nil {
				summaryErr = terminalErr
			}
		}
	}
	if cfg.CleanupIsolationDuration > 0 {
		if cleanupIsolationStats.Requests == 0 {
			failed = true
			if summaryErr == nil {
				summaryErr = status.Error(codes.FailedPrecondition, "post-switch observation window had no probe samples")
			}
		}
		if cleanupIsolationStats.ServiceErrors > 0 {
			failed = true
			if summaryErr == nil {
				summaryErr = status.Errorf(codes.FailedPrecondition, "service_errors_during_post_switch_observation=%d", cleanupIsolationStats.ServiceErrors)
			}
		}
	}
	if routeSwitchedErr := validateRouteSwitchedIsolationStats(routeSwitchedStats, cfg.RequireRouteSwitchedProbe); routeSwitchedErr != nil {
		failed = true
		if summaryErr == nil {
			summaryErr = routeSwitchedErr
		}
	}
	if cfg.MutationInterval > 0 {
		if mutationRouteSwitchedErr := validateMutationRouteSwitchedIsolationStats(mutationRouteSwitchedStats, cfg.RequireRouteSwitchedProbe); mutationRouteSwitchedErr != nil {
			failed = true
			if summaryErr == nil {
				summaryErr = mutationRouteSwitchedErr
			}
		}
	}
	migrationWindowResourceResult, endToEndResourceResult := hotMigrationResourceResults(cfg.SkipFinalActorCleanup, cleanupErr)
	if cleanupErr != nil {
		failed = true
		if summaryErr == nil {
			summaryErr = cleanupErr
		}
	}
	if resourceErr := validateEndToEndResourceProof(consistencyProbe, mutationMaxCounter); resourceErr != nil {
		failed = true
		migrationWindowResourceResult, endToEndResourceResult = hotMigrationResourceResults(cfg.SkipFinalActorCleanup, resourceErr)
		if summaryErr == nil {
			summaryErr = resourceErr
		}
	}
	if summaryErr != nil {
		failed = true
	}
	terminalEchoLatencyMax, terminalEchoLatencyP95 := terminalEchoLatencyStats(terminalStats)
	summary := perfdata.LifecycleEvent{
		RunID:                                    cfg.RunID,
		OperationID:                              fmt.Sprintf("%s-%03d-hot-migration-summary", actor, i),
		ActorID:                                  cfg.Atespace + "/" + actor,
		Operation:                                "hot_migration_summary",
		Stage:                                    cfg.Stage,
		StartTS:                                  migrationStart.UTC().Format(time.RFC3339Nano),
		EndTS:                                    observationEndTime.UTC().Format(time.RFC3339Nano),
		DurationMS:                               observationEndTime.Sub(migrationStart).Milliseconds(),
		Workload:                                 cfg.Workload,
		Round:                                    cfg.Round,
		NodeScale:                                cfg.NodeScale,
		Concurrency:                              cfg.Concurrency,
		Result:                                   "ok",
		HTTPStatus:                               summaryProbe.StatusCode,
		HTTPChecksum:                             summaryProbe.Checksum,
		HTTPBodyBytes:                            summaryProbe.BodyBytes,
		HTTPIdempotencyKey:                       summaryProbe.IdempotencyKey,
		HTTPCounter:                              summaryProbe.Counter,
		HTTPStateVersion:                         summaryProbe.StateVersion,
		HTTPLastMutationID:                       summaryProbe.LastMutationID,
		HTTPInstanceID:                           summaryProbe.InstanceID,
		HTTPBootID:                               summaryProbe.BootID,
		HTTPPodName:                              summaryProbe.PodName,
		HTTPNodeName:                             summaryProbe.NodeName,
		HTTPRouteGeneration:                      summaryProbe.RouteGeneration,
		HTTPRoutePhase:                           summaryProbe.RoutePhase,
		HTTPRouteTargetWorker:                    summaryProbe.RouteTargetWorker,
		HTTPRouteTargetNode:                      summaryProbe.RouteTargetNode,
		HTTPRouteTargetIP:                        summaryProbe.RouteTargetIP,
		RouteGenerationBefore:                    routeGenerationBefore,
		RouteGenerationAfter:                     routeGenerationAfter,
		LongestSuccessGapMS:                      stats.longestSuccessGap.Milliseconds(),
		MaxRequestStartGapMS:                     stats.maxRequestStartGap.Milliseconds(),
		MaxSuccessCompletionGapMS:                stats.maxSuccessCompletionGap.Milliseconds(),
		MaxSingleRequestLatencyMS:                stats.maxSingleRequestLatency.Milliseconds(),
		TotalToTargetSuccessMS:                   afterEnd.Sub(migrationStart).Milliseconds(),
		ProbeRequests:                            stats.requests,
		ProbeSuccesses:                           stats.successes,
		ProbeFailures:                            stats.failures,
		ProbeStatusCounts:                        stats.statusCounts,
		ProbeCanceledByStop:                      stats.canceledByStop,
		ProbeCanceledStartedBeforeStop:           stats.canceledStartedBeforeStop,
		ProbeInflightAtStopCount:                 stats.inflightAtStopCount,
		ProbeOldestInflightAgeAtStopMS:           stats.oldestInflightAgeAtStop.Milliseconds(),
		MissingRouteProofCount:                   stats.missingRouteProofCount + mutationStats.missingRouteProofCount,
		MutationRequests:                         mutationStats.requests,
		MutationSuccesses:                        mutationStats.successes,
		MutationFailures:                         mutationStats.failures,
		MutationCanceledByStop:                   mutationStats.canceledByStop,
		MutationCanceledStartedBeforeStop:        mutationStats.canceledStartedBeforeStop,
		MutationInflightAtStopCount:              mutationStats.inflightAtStopCount,
		MutationOldestInflightAgeAtStopMS:        mutationStats.oldestInflightAgeAtStop.Milliseconds(),
		MutationMaxAcceptedCounter:               mutationMaxCounter,
		MutationDuplicateCount:                   mutationDuplicateCount,
		MutationLostAcceptedCount:                mutationLostCount,
		StateRegressed:                           stateRegressed(beforeProbe, afterProbe) || stats.monotonicViolationCount > 0,
		MonotonicViolationCount:                  stats.monotonicViolationCount,
		StaleVersionCount:                        staleVersionCount,
		MissingStateProofCount:                   stats.missingStateProofCount + mutationStats.missingStateProofCount,
		DualActiveObserved:                       dualActive,
		MigrationWindowResult:                    "ok",
		MigrationWindowResourceResult:            migrationWindowResourceResult,
		EndToEndResourceResult:                   endToEndResourceResult,
		CleanupIsolationDurationMS:               cfg.CleanupIsolationDuration.Milliseconds(),
		CleanupIsolationProbeRequests:            cleanupIsolationStats.Requests,
		CleanupIsolationProbeSuccesses:           cleanupIsolationStats.Successes,
		ServiceErrorsDuringCleanup:               cleanupIsolationStats.ServiceErrors,
		RouteSwitchedProbeRequests:               routeSwitchedStats.Requests,
		RouteSwitchedProbeSuccesses:              routeSwitchedStats.Successes,
		ServiceErrorsDuringRouteSwitched:         routeSwitchedStats.ServiceErrors,
		RouteTargetMismatchDuringRouteSwitched:   routeSwitchedStats.TargetMismatches,
		MissingRouteProofDuringRouteSwitched:     routeSwitchedStats.MissingRouteProof,
		MutationRouteSwitchedProbeRequests:       mutationRouteSwitchedStats.Requests,
		MutationRouteSwitchedProbeSuccesses:      mutationRouteSwitchedStats.Successes,
		MutationServiceErrorsDuringRouteSwitched: mutationRouteSwitchedStats.ServiceErrors,
		MutationRouteTargetMismatchDuringRouteSwitched: mutationRouteSwitchedStats.TargetMismatches,
		MutationMissingRouteProofDuringRouteSwitched:   mutationRouteSwitchedStats.MissingRouteProof,
		StreamEvents:                           sseStats.events,
		StreamStartedBeforePrepare:             sseStats.startedBeforePrepare,
		StreamEventsBeforePrepare:              sseStats.eventsBeforePrepare,
		StreamDisconnects:                      sseStats.disconnects,
		StreamMaxMessageGapMS:                  sseStats.maxMessageGap.Milliseconds(),
		StreamGapThresholdViolation:            sseStats.gapThresholdViolation,
		StreamSequenceGapCount:                 sseStats.sequenceGapCount,
		StreamDuplicateCount:                   sseStats.duplicateCount,
		StreamStateRegressionCount:             sseStats.stateRegressionCount,
		StreamMaxGapPreviousFrame:              sseFrameEvidence(migrationStart, sseStats.maxMessageGapPrevious),
		StreamMaxGapCurrentFrame:               sseFrameEvidence(migrationStart, sseStats.maxMessageGapCurrent),
		TerminalMessages:                       terminalStats.messages,
		TerminalStartedBeforePrepare:           terminalStats.startedBeforePrepare,
		TerminalTicks:                          terminalStats.ticks,
		TerminalEchoes:                         terminalStats.echoes,
		TerminalExpectedEchoes:                 terminalStats.expectedEchoes,
		TerminalDisconnects:                    terminalStats.disconnects,
		TerminalReconnectsObserved:             terminalStats.disconnects,
		TerminalUXGapObserved:                  terminalUXGapObserved(terminalStats),
		TerminalMaxMessageGapMS:                terminalStats.maxMessageGap.Milliseconds(),
		TerminalMaxMessageGapStartOffsetMS:     terminalOffsetMS(migrationStart, terminalStats.maxMessageGapStart),
		TerminalMaxMessageGapEndOffsetMS:       terminalOffsetMS(migrationStart, terminalStats.maxMessageGapEnd),
		TerminalEchoLatencyMaxMS:               terminalEchoLatencyMax.Milliseconds(),
		TerminalEchoLatencyP95MS:               terminalEchoLatencyP95.Milliseconds(),
		TerminalEchoLatencyMaxSequence:         terminalStats.maxEchoLatencySequence,
		TerminalEchoLatencyMaxSentOffsetMS:     terminalOffsetMS(migrationStart, terminalStats.maxEchoLatencySentAt),
		TerminalEchoLatencyMaxReceivedOffsetMS: terminalOffsetMS(migrationStart, terminalStats.maxEchoLatencyReceivedAt),
		TerminalGapThresholdViolation:          terminalStats.gapThresholdViolation,
		TerminalSequenceGapCount:               terminalStats.sequenceGapCount,
		TerminalDuplicateCount:                 terminalStats.duplicateCount,
		TerminalMissingEchoCount:               terminalStats.missingEchoCount,
		TerminalMissingEchoSequences:           terminalStats.missingEchoSequences,
		TerminalMissingEchoSentOffsetsMS:       terminalMissingEchoSentOffsets(migrationStart, terminalStats),
		TerminalPIDChanged:                     terminalStats.pidChanged,
		TerminalStateRegressionCount:           terminalStats.stateRegressionCount,
		TerminalContinuityResult:               terminalContinuityResult(terminalStats),
		TerminalUXResult:                       terminalUXResult(terminalStats),
		TerminalMaxGapPreviousFrame:            terminalFrameEvidence(migrationStart, terminalStats.maxMessageGapPrevious),
		TerminalMaxGapCurrentFrame:             terminalFrameEvidence(migrationStart, terminalStats.maxMessageGapCurrent),
		TerminalEchoLatencyMaxFrame:            terminalFrameEvidence(migrationStart, terminalStats.maxEchoLatencyFrame),
		TerminalStateRegressionSamples:         terminalStateRegressionEvidence(migrationStart, terminalStats.stateRegressionSamples),
		StageDurationMS:                        commitStageDurationMS,
	}
	if len(terminalStats.missingEchoSequences) > 0 {
		summary.TerminalFirstMissingEchoSequence = terminalStats.missingEchoSequences[0]
		summary.TerminalLastMissingEchoSequence = terminalStats.missingEchoSequences[len(terminalStats.missingEchoSequences)-1]
	}
	applyHotMigrationConfigEvidence(&summary, cfg)
	applyPlacementToEvent(&summary, source, target)
	if summaryErr != nil {
		summary.Result = "error"
		summary.MigrationWindowResult = "error"
		summary.MigrationWindowResourceResult = "error"
		summary.EndToEndResourceResult = "error"
		summary.ErrorCode = status.Code(summaryErr).String()
		summary.ErrorReason = summaryErr.Error()
		summary.VerdictReason = summaryErr.Error()
	}
	write(summary)
	if cfg.SkipFinalActorCleanup {
		return failed
	}

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

func failedHotMigrationSummaryEvent(cfg HotMigrationConfig, actor string, sampleIndex int, start, end time.Time, source, target workerPlacement, routeGenerationBefore, routeGenerationAfter int64, beforeProbe httpProbeResult, stats, mutationStats continuousProbeStats, sseStats sseProbeStats, terminalStats terminalWebSocketStats, stageDurationMS map[string]int64, summaryErr error) perfdata.LifecycleEvent {
	mutationMaxCounter := maxSuccessCounter(mutationStats.samples)
	routeSwitchedStats := routeSwitchedIsolationStats(stats, target)
	mutationRouteSwitchedStats := routeSwitchedIsolationStats(mutationStats, target)
	terminalEchoLatencyMax, terminalEchoLatencyP95 := terminalEchoLatencyStats(terminalStats)
	summary := perfdata.LifecycleEvent{
		RunID:                                    cfg.RunID,
		OperationID:                              fmt.Sprintf("%s-%03d-hot-migration-summary", actor, sampleIndex),
		ActorID:                                  cfg.Atespace + "/" + actor,
		Operation:                                "hot_migration_summary",
		Stage:                                    cfg.Stage,
		StartTS:                                  start.UTC().Format(time.RFC3339Nano),
		EndTS:                                    end.UTC().Format(time.RFC3339Nano),
		DurationMS:                               end.Sub(start).Milliseconds(),
		Workload:                                 cfg.Workload,
		Round:                                    cfg.Round,
		NodeScale:                                cfg.NodeScale,
		Concurrency:                              cfg.Concurrency,
		Result:                                   "error",
		ErrorCode:                                status.Code(summaryErr).String(),
		ErrorReason:                              summaryErr.Error(),
		VerdictReason:                            summaryErr.Error(),
		HTTPStatus:                               beforeProbe.StatusCode,
		HTTPChecksum:                             beforeProbe.Checksum,
		HTTPBodyBytes:                            beforeProbe.BodyBytes,
		HTTPCounter:                              beforeProbe.Counter,
		HTTPStateVersion:                         beforeProbe.StateVersion,
		HTTPLastMutationID:                       beforeProbe.LastMutationID,
		HTTPInstanceID:                           beforeProbe.InstanceID,
		HTTPBootID:                               beforeProbe.BootID,
		HTTPPodName:                              beforeProbe.PodName,
		HTTPNodeName:                             beforeProbe.NodeName,
		HTTPRouteGeneration:                      beforeProbe.RouteGeneration,
		HTTPRoutePhase:                           beforeProbe.RoutePhase,
		HTTPRouteTargetWorker:                    beforeProbe.RouteTargetWorker,
		HTTPRouteTargetNode:                      beforeProbe.RouteTargetNode,
		HTTPRouteTargetIP:                        beforeProbe.RouteTargetIP,
		RouteGenerationBefore:                    routeGenerationBefore,
		RouteGenerationAfter:                     routeGenerationAfter,
		LongestSuccessGapMS:                      stats.longestSuccessGap.Milliseconds(),
		MaxRequestStartGapMS:                     stats.maxRequestStartGap.Milliseconds(),
		MaxSuccessCompletionGapMS:                stats.maxSuccessCompletionGap.Milliseconds(),
		MaxSingleRequestLatencyMS:                stats.maxSingleRequestLatency.Milliseconds(),
		TotalToTargetSuccessMS:                   end.Sub(start).Milliseconds(),
		ProbeRequests:                            stats.requests,
		ProbeSuccesses:                           stats.successes,
		ProbeFailures:                            stats.failures,
		ProbeStatusCounts:                        stats.statusCounts,
		ProbeCanceledByStop:                      stats.canceledByStop,
		ProbeCanceledStartedBeforeStop:           stats.canceledStartedBeforeStop,
		ProbeInflightAtStopCount:                 stats.inflightAtStopCount,
		ProbeOldestInflightAgeAtStopMS:           stats.oldestInflightAgeAtStop.Milliseconds(),
		MissingRouteProofCount:                   stats.missingRouteProofCount + mutationStats.missingRouteProofCount,
		MutationRequests:                         mutationStats.requests,
		MutationSuccesses:                        mutationStats.successes,
		MutationFailures:                         mutationStats.failures,
		MutationCanceledByStop:                   mutationStats.canceledByStop,
		MutationCanceledStartedBeforeStop:        mutationStats.canceledStartedBeforeStop,
		MutationInflightAtStopCount:              mutationStats.inflightAtStopCount,
		MutationOldestInflightAgeAtStopMS:        mutationStats.oldestInflightAgeAtStop.Milliseconds(),
		MutationMaxAcceptedCounter:               mutationMaxCounter,
		MonotonicViolationCount:                  stats.monotonicViolationCount,
		MissingStateProofCount:                   stats.missingStateProofCount + mutationStats.missingStateProofCount,
		MigrationWindowResult:                    "error",
		MigrationWindowResourceResult:            "error",
		EndToEndResourceResult:                   "error",
		RouteSwitchedProbeRequests:               routeSwitchedStats.Requests,
		RouteSwitchedProbeSuccesses:              routeSwitchedStats.Successes,
		ServiceErrorsDuringRouteSwitched:         routeSwitchedStats.ServiceErrors,
		RouteTargetMismatchDuringRouteSwitched:   routeSwitchedStats.TargetMismatches,
		MissingRouteProofDuringRouteSwitched:     routeSwitchedStats.MissingRouteProof,
		MutationRouteSwitchedProbeRequests:       mutationRouteSwitchedStats.Requests,
		MutationRouteSwitchedProbeSuccesses:      mutationRouteSwitchedStats.Successes,
		MutationServiceErrorsDuringRouteSwitched: mutationRouteSwitchedStats.ServiceErrors,
		MutationRouteTargetMismatchDuringRouteSwitched: mutationRouteSwitchedStats.TargetMismatches,
		MutationMissingRouteProofDuringRouteSwitched:   mutationRouteSwitchedStats.MissingRouteProof,
		StreamEvents:                           sseStats.events,
		StreamStartedBeforePrepare:             sseStats.startedBeforePrepare,
		StreamEventsBeforePrepare:              sseStats.eventsBeforePrepare,
		StreamDisconnects:                      sseStats.disconnects,
		StreamMaxMessageGapMS:                  sseStats.maxMessageGap.Milliseconds(),
		StreamGapThresholdViolation:            sseStats.gapThresholdViolation,
		StreamSequenceGapCount:                 sseStats.sequenceGapCount,
		StreamDuplicateCount:                   sseStats.duplicateCount,
		StreamStateRegressionCount:             sseStats.stateRegressionCount,
		StreamMaxGapPreviousFrame:              sseFrameEvidence(start, sseStats.maxMessageGapPrevious),
		StreamMaxGapCurrentFrame:               sseFrameEvidence(start, sseStats.maxMessageGapCurrent),
		TerminalMessages:                       terminalStats.messages,
		TerminalStartedBeforePrepare:           terminalStats.startedBeforePrepare,
		TerminalTicks:                          terminalStats.ticks,
		TerminalEchoes:                         terminalStats.echoes,
		TerminalExpectedEchoes:                 terminalStats.expectedEchoes,
		TerminalDisconnects:                    terminalStats.disconnects,
		TerminalReconnectsObserved:             terminalStats.disconnects,
		TerminalUXGapObserved:                  terminalUXGapObserved(terminalStats),
		TerminalMaxMessageGapMS:                terminalStats.maxMessageGap.Milliseconds(),
		TerminalMaxMessageGapStartOffsetMS:     terminalOffsetMS(start, terminalStats.maxMessageGapStart),
		TerminalMaxMessageGapEndOffsetMS:       terminalOffsetMS(start, terminalStats.maxMessageGapEnd),
		TerminalEchoLatencyMaxMS:               terminalEchoLatencyMax.Milliseconds(),
		TerminalEchoLatencyP95MS:               terminalEchoLatencyP95.Milliseconds(),
		TerminalEchoLatencyMaxSequence:         terminalStats.maxEchoLatencySequence,
		TerminalEchoLatencyMaxSentOffsetMS:     terminalOffsetMS(start, terminalStats.maxEchoLatencySentAt),
		TerminalEchoLatencyMaxReceivedOffsetMS: terminalOffsetMS(start, terminalStats.maxEchoLatencyReceivedAt),
		TerminalGapThresholdViolation:          terminalStats.gapThresholdViolation,
		TerminalSequenceGapCount:               terminalStats.sequenceGapCount,
		TerminalDuplicateCount:                 terminalStats.duplicateCount,
		TerminalMissingEchoCount:               terminalStats.missingEchoCount,
		TerminalMissingEchoSequences:           terminalStats.missingEchoSequences,
		TerminalMissingEchoSentOffsetsMS:       terminalMissingEchoSentOffsets(start, terminalStats),
		TerminalPIDChanged:                     terminalStats.pidChanged,
		TerminalStateRegressionCount:           terminalStats.stateRegressionCount,
		TerminalContinuityResult:               terminalContinuityResult(terminalStats),
		TerminalUXResult:                       terminalUXResult(terminalStats),
		TerminalMaxGapPreviousFrame:            terminalFrameEvidence(start, terminalStats.maxMessageGapPrevious),
		TerminalMaxGapCurrentFrame:             terminalFrameEvidence(start, terminalStats.maxMessageGapCurrent),
		TerminalEchoLatencyMaxFrame:            terminalFrameEvidence(start, terminalStats.maxEchoLatencyFrame),
		TerminalStateRegressionSamples:         terminalStateRegressionEvidence(start, terminalStats.stateRegressionSamples),
		StageDurationMS:                        stageDurationMS,
	}
	if len(terminalStats.missingEchoSequences) > 0 {
		summary.TerminalFirstMissingEchoSequence = terminalStats.missingEchoSequences[0]
		summary.TerminalLastMissingEchoSequence = terminalStats.missingEchoSequences[len(terminalStats.missingEchoSequences)-1]
	}
	applyHotMigrationConfigEvidence(&summary, cfg)
	applyPlacementToEvent(&summary, source, target)
	return summary
}

func applyHotMigrationConfigEvidence(ev *perfdata.LifecycleEvent, cfg HotMigrationConfig) {
	ev.LiveMigrationDowntimeMS = cfg.LiveMigrationDowntimeMS
	ev.LiveMigrationConnections = cfg.LiveMigrationConnections
	ev.LiveMigrationMemoryMode = cfg.LiveMigrationMemoryMode
}

func hotMigrationResourceResults(skipFinalActorCleanup bool, resourceErr error) (string, string) {
	if resourceErr != nil {
		return "error", "error"
	}
	if skipFinalActorCleanup {
		return "ok", "skipped"
	}
	return "ok", "pending_cleanup"
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

func mergeStageDurationMS(maps ...map[string]int64) map[string]int64 {
	var out map[string]int64
	for _, in := range maps {
		for k, v := range in {
			if out == nil {
				out = map[string]int64{}
			}
			out[k] = v
		}
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
	if !hasStateProof(beforeProbe) {
		return status.Errorf(codes.FailedPrecondition, "missing before-migration state proof: counter=%d state_version=%d last_mutation_id=%q", beforeProbe.Counter, beforeProbe.StateVersion, beforeProbe.LastMutationID)
	}
	if !hasStateProof(afterProbe) {
		return status.Errorf(codes.FailedPrecondition, "missing after-migration state proof: counter=%d state_version=%d last_mutation_id=%q", afterProbe.Counter, afterProbe.StateVersion, afterProbe.LastMutationID)
	}
	if requireCrossNode {
		if source.node == "" || target.node == "" {
			return status.Errorf(codes.FailedPrecondition, "missing source/target node proof: source=%q target=%q", source.node, target.node)
		}
		if source.node == target.node {
			return status.Errorf(codes.FailedPrecondition, "source and target are on the same node %s", source.node)
		}
	}
	if afterGen <= 0 {
		return status.Errorf(codes.FailedPrecondition, "missing after-migration route generation proof: before=%d after=%d", beforeGen, afterGen)
	}
	if afterGen <= beforeGen {
		return status.Errorf(codes.FailedPrecondition, "route generation did not increase: before=%d after=%d", beforeGen, afterGen)
	}
	if afterProbe.RouteTargetWorker == "" || afterProbe.RouteTargetNode == "" || afterProbe.RouteTargetIP == "" {
		return status.Errorf(codes.FailedPrecondition, "missing after-migration route target proof: worker=%q node=%q ip=%q", afterProbe.RouteTargetWorker, afterProbe.RouteTargetNode, afterProbe.RouteTargetIP)
	}
	if target.key() != "" && afterProbe.RouteTargetWorker != target.key() {
		return status.Errorf(codes.FailedPrecondition, "after-migration route target worker mismatch: got=%q want=%q", afterProbe.RouteTargetWorker, target.key())
	}
	if target.node != "" && afterProbe.RouteTargetNode != target.node {
		return status.Errorf(codes.FailedPrecondition, "after-migration route target node mismatch: got=%q want=%q", afterProbe.RouteTargetNode, target.node)
	}
	if beforeProbe.Checksum != "" && afterProbe.Checksum != "" && beforeProbe.Checksum != afterProbe.Checksum {
		return fmt.Errorf("checksum changed across hot migration: before=%s after=%s", beforeProbe.Checksum, afterProbe.Checksum)
	}
	if stateRegressed(beforeProbe, afterProbe) {
		return fmt.Errorf("state regressed across hot migration: before_counter=%d after_counter=%d before_version=%d after_version=%d", beforeProbe.Counter, afterProbe.Counter, beforeProbe.StateVersion, afterProbe.StateVersion)
	}
	return nil
}

func stateRegressed(before, after httpProbeResult) bool {
	if after.Counter < before.Counter {
		return true
	}
	if before.StateVersion > 0 && after.StateVersion < before.StateVersion {
		return true
	}
	return false
}

func hasStateProof(result httpProbeResult) bool {
	return result.Counter > 0 && result.StateVersion > 0 && result.LastMutationID != ""
}

func hasRouteProof(result httpProbeResult) bool {
	return result.RouteGeneration > 0 && result.RouteTargetWorker != "" && result.RouteTargetNode != "" && result.RouteTargetIP != ""
}

func validateEndToEndResourceProof(result httpProbeResult, minCounter int64) error {
	if result.StatusCode != http.StatusOK {
		return status.Errorf(codes.FailedPrecondition, "final resource HTTP status=%d, want 200", result.StatusCode)
	}
	if !result.Ready {
		return status.Errorf(codes.FailedPrecondition, "final resource is not ready")
	}
	if result.Checksum == "" {
		return status.Errorf(codes.FailedPrecondition, "missing final resource checksum")
	}
	if result.ResidentLen <= 0 || result.TargetMemoryBytes <= 0 || result.ResidentLen != result.TargetMemoryBytes {
		return status.Errorf(codes.FailedPrecondition, "missing resident memory proof: resident_len=%d target_memory_bytes=%d", result.ResidentLen, result.TargetMemoryBytes)
	}
	if !hasStateProof(result) {
		return status.Errorf(codes.FailedPrecondition, "missing final resource state proof: counter=%d state_version=%d last_mutation_id=%q", result.Counter, result.StateVersion, result.LastMutationID)
	}
	if !hasRouteProof(result) {
		return status.Errorf(codes.FailedPrecondition, "missing final resource route proof: generation=%d worker=%q node=%q ip=%q", result.RouteGeneration, result.RouteTargetWorker, result.RouteTargetNode, result.RouteTargetIP)
	}
	if minCounter > 0 && result.Counter < minCounter {
		return status.Errorf(codes.FailedPrecondition, "final counter %d is below max accepted mutation counter %d", result.Counter, minCounter)
	}
	return nil
}

func validateContinuousProbeStats(stats continuousProbeStats, staleVersionCount int, dualActive bool) error {
	var violations []string
	if stats.requests == 0 {
		violations = append(violations, "requests=0")
	}
	if stats.failures > 0 {
		violations = append(violations, fmt.Sprintf("failures=%d", stats.failures))
	}
	if stats.monotonicViolationCount > 0 {
		violations = append(violations, fmt.Sprintf("monotonic_violations=%d", stats.monotonicViolationCount))
	}
	if stats.missingStateProofCount > 0 {
		violations = append(violations, fmt.Sprintf("missing_state_proof=%d", stats.missingStateProofCount))
	}
	if stats.missingRouteProofCount > 0 {
		violations = append(violations, fmt.Sprintf("missing_route_proof=%d", stats.missingRouteProofCount))
	}
	if staleVersionCount > 0 {
		violations = append(violations, fmt.Sprintf("stale_versions=%d", staleVersionCount))
	}
	if dualActive {
		violations = append(violations, "dual_active=true")
	}
	if len(violations) > 0 {
		return fmt.Errorf("continuous probe violation: %s", strings.Join(violations, ", "))
	}
	return nil
}

func validateExtraProbeStats(config extraProbeConfig, stats continuousProbeStats, baseline httpProbeResult) error {
	staleVersionCount := countStaleProbeVersions(stats.samples, baseline)
	dualActive := dualActiveObserved(stats.samples)
	if err := validateContinuousProbeStats(stats, staleVersionCount, dualActive); err != nil {
		return fmt.Errorf("extra probe %q path %q violation: %w", config.Label, config.Path, err)
	}
	return nil
}

func validateContinuousMutationStats(stats continuousProbeStats, duplicateCount, lostAcceptedCount int) error {
	var violations []string
	if stats.requests == 0 {
		violations = append(violations, "mutation_requests=0")
	}
	if stats.failures > 0 {
		violations = append(violations, fmt.Sprintf("mutation_failures=%d", stats.failures))
	}
	if stats.missingStateProofCount > 0 {
		violations = append(violations, fmt.Sprintf("mutation_missing_state_proof=%d", stats.missingStateProofCount))
	}
	if stats.missingRouteProofCount > 0 {
		violations = append(violations, fmt.Sprintf("mutation_missing_route_proof=%d", stats.missingRouteProofCount))
	}
	if stats.monotonicViolationCount > 0 {
		violations = append(violations, fmt.Sprintf("mutation_monotonic_violations=%d", stats.monotonicViolationCount))
	}
	if duplicateCount > 0 {
		violations = append(violations, fmt.Sprintf("mutation_duplicate_counters=%d", duplicateCount))
	}
	if lostAcceptedCount > 0 {
		violations = append(violations, fmt.Sprintf("mutation_lost_accepted=%d", lostAcceptedCount))
	}
	if len(violations) > 0 {
		return fmt.Errorf("continuous mutation violation: %s", strings.Join(violations, ", "))
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
	StatusCode        int
	Ready             bool
	Checksum          string
	BodyBytes         int64
	ResidentLen       int64
	TargetMemoryBytes int64
	IdempotencyKey    string
	Counter           int64
	StateVersion      int64
	LastMutationID    string
	InstanceID        string
	BootID            string
	PodName           string
	NodeName          string
	RouteGeneration   int64
	RoutePhase        string
	RouteTargetWorker string
	RouteTargetNode   string
	RouteTargetIP     string
	Body              string
	Stale             bool
}

func parseHTTPProbeBody(body []byte) httpProbeResult {
	result := httpProbeResult{Body: strings.TrimSpace(string(body))}
	if len(result.Body) > 512 {
		result.Body = result.Body[:512]
	}
	var payload struct {
		Ready             bool   `json:"ready"`
		Checksum          string `json:"checksum"`
		ResidentLen       int64  `json:"resident_len"`
		TargetMemoryBytes int64  `json:"target_memory_bytes"`
		Counter           int64  `json:"counter"`
		StateVersion      int64  `json:"state_version"`
		LastMutationID    string `json:"last_mutation_id"`
		InstanceID        string `json:"instance_id"`
		BootID            string `json:"boot_id"`
		PodName           string `json:"pod_name"`
		NodeName          string `json:"node_name"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		result.Ready = payload.Ready
		result.Checksum = payload.Checksum
		result.ResidentLen = payload.ResidentLen
		result.TargetMemoryBytes = payload.TargetMemoryBytes
		result.Counter = payload.Counter
		result.StateVersion = payload.StateVersion
		result.LastMutationID = payload.LastMutationID
		result.InstanceID = payload.InstanceID
		result.BootID = payload.BootID
		result.PodName = payload.PodName
		result.NodeName = payload.NodeName
	}
	return result
}

func applyHTTPProbeHeaders(result *httpProbeResult, header http.Header) {
	if result.Checksum == "" {
		result.Checksum = header.Get("X-Substrate-State-Checksum")
	}
	if result.Counter == 0 {
		if counter, err := strconv.ParseInt(header.Get("X-Substrate-State-Counter"), 10, 64); err == nil {
			result.Counter = counter
		}
	}
	if result.StateVersion == 0 {
		if version, err := strconv.ParseInt(header.Get("X-Substrate-State-Version"), 10, 64); err == nil {
			result.StateVersion = version
		}
	}
	if result.LastMutationID == "" {
		result.LastMutationID = header.Get("X-Substrate-State-Last-Mutation-ID")
	}
	if result.BootID == "" {
		result.BootID = header.Get("X-Substrate-State-Boot-ID")
	}
	if result.InstanceID == "" {
		result.InstanceID = header.Get("X-Substrate-State-Instance-ID")
	}
	if result.PodName == "" {
		result.PodName = header.Get("X-Substrate-State-Pod-Name")
	}
	if result.NodeName == "" {
		result.NodeName = header.Get("X-Substrate-State-Node-Name")
	}
	if raw := header.Get("X-Substrate-Route-Generation"); raw != "" {
		if generation, err := strconv.ParseInt(raw, 10, 64); err == nil {
			result.RouteGeneration = generation
		}
	}
	result.RoutePhase = header.Get("X-Substrate-Route-Phase")
	result.RouteTargetWorker = header.Get("X-Substrate-Route-Target-Worker")
	result.RouteTargetNode = header.Get("X-Substrate-Route-Target-Node")
	result.RouteTargetIP = header.Get("X-Substrate-Route-Target-IP")
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
	baseURL        string
	client         *http.Client
	stopCh         chan struct{}
	readLimitBytes int64
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
	return p.DoWithHeaders(ctx, method, atespace, actor, path, nil)
}

func (p *routerHTTPProber) DoWithHeaders(ctx context.Context, method, atespace, actor, path string, headers map[string]string) (httpProbeResult, error) {
	return p.DoWithHeadersAndBody(ctx, method, atespace, actor, path, headers, nil)
}

func (p *routerHTTPProber) DoWithHeadersAndBody(ctx context.Context, method, atespace, actor, path string, headers map[string]string, body []byte) (httpProbeResult, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path, reader)
	if err != nil {
		return httpProbeResult{}, err
	}
	req.Method = method
	req.Host = resources.ActorDNSName(atespace, actor)
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	if body != nil {
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return httpProbeResult{}, err
	}
	defer resp.Body.Close()
	limit := p.readLimitBytes
	if limit <= 0 {
		limit = 64 * 1024
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, limit))
	result := parseHTTPProbeBody(responseBody)
	result.BodyBytes = int64(len(responseBody))
	result.StatusCode = resp.StatusCode
	result.Stale = resp.Header.Get("X-Substrate-Stale") == "true"
	applyHTTPProbeHeaders(&result, resp.Header)
	if readErr != nil {
		return result, readErr
	}
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("HTTP status=%d body=%q", resp.StatusCode, result.Body)
	}
	return result, nil
}

func (p *routerHTTPProber) Stream(ctx context.Context, atespace, actor, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Host = resources.ActorDNSName(atespace, actor)
	req.Header.Set("Accept", "text/event-stream")
	client := *p.client
	client.Timeout = 0
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("SSE HTTP status=%d body=%q", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func (p *routerHTTPProber) WebSocket(ctx context.Context, atespace, actor, path string) (*websocket.Conn, error) {
	u, err := url.Parse(p.baseURL + path)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return nil, fmt.Errorf("unsupported router base URL scheme %q for websocket", u.Scheme)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 30 * time.Second}
	header := http.Header{}
	header.Set("Host", resources.ActorDNSName(atespace, actor))
	conn, _, err := dialer.DialContext(ctx, u.String(), header)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type continuousProbeStats struct {
	requests                  int
	successes                 int
	failures                  int
	canceledByStop            int
	canceledStartedBeforeStop int
	inflightAtStopCount       int
	oldestInflightAgeAtStop   time.Duration
	statusCounts              map[string]int
	longestSuccessGap         time.Duration
	maxRequestStartGap        time.Duration
	maxSuccessCompletionGap   time.Duration
	maxSingleRequestLatency   time.Duration
	monotonicViolationCount   int
	missingStateProofCount    int
	missingRouteProofCount    int
	samples                   []continuousProbeSample
}

type continuousProbeRun struct {
	done <-chan continuousProbeStats
}

type extraProbeRun struct {
	config extraProbeConfig
	run    continuousProbeRun
}

type extraProbeStats struct {
	config extraProbeConfig
	stats  continuousProbeStats
}

type continuousProbeSample struct {
	sequence       int
	start          time.Time
	end            time.Time
	result         httpProbeResult
	err            error
	canceledByStop bool
}

type postSwitchIsolationProbeStats struct {
	Requests      int
	Successes     int
	ServiceErrors int
}

type routeSwitchedIsolationProbeStats struct {
	Requests          int
	Successes         int
	ServiceErrors     int
	TargetMismatches  int
	MissingRouteProof int
}

func startContinuousProbe(ctx context.Context, prober *routerHTTPProber, atespace, actor, path string, interval time.Duration) continuousProbeRun {
	return startContinuousProbeWithDo(ctx, func(ctx context.Context) (httpProbeResult, error) {
		return prober.Probe(ctx, atespace, actor, path)
	}, interval)
}

func parseExtraProbeConfigs(paths []string) ([]extraProbeConfig, error) {
	var configs []extraProbeConfig
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		label := fmt.Sprintf("extra-%d", len(configs)+1)
		path := raw
		if before, after, ok := strings.Cut(raw, "="); ok {
			label = strings.TrimSpace(before)
			path = strings.TrimSpace(after)
		}
		if label == "" {
			return nil, fmt.Errorf("extra probe path %q has empty label", raw)
		}
		if !strings.HasPrefix(path, "/") {
			return nil, fmt.Errorf("extra probe path %q must start with /", raw)
		}
		configs = append(configs, extraProbeConfig{Label: label, Path: path})
	}
	return configs, nil
}

func parsePreMigrationPostConfigs(paths []string) ([]preMigrationPostConfig, error) {
	var configs []preMigrationPostConfig
	unlabeled := 0
	for _, raw := range paths {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		unlabeled++
		label := fmt.Sprintf("post-%d", unlabeled)
		path := raw
		if before, after, ok := strings.Cut(raw, "="); ok {
			label = strings.TrimSpace(before)
			path = strings.TrimSpace(after)
			unlabeled--
		}
		if label == "" {
			return nil, fmt.Errorf("pre-migration post path %q has empty label", raw)
		}
		if !strings.HasPrefix(path, "/") {
			return nil, fmt.Errorf("pre-migration post path %q must start with /", raw)
		}
		configs = append(configs, preMigrationPostConfig{Label: label, Path: path})
	}
	return configs, nil
}

func startExtraProbeRuns(ctx context.Context, prober *routerHTTPProber, atespace, actor string, configs []extraProbeConfig, interval time.Duration) []extraProbeRun {
	runs := make([]extraProbeRun, 0, len(configs))
	for _, cfg := range configs {
		runs = append(runs, extraProbeRun{
			config: cfg,
			run:    startContinuousProbe(ctx, prober, atespace, actor, cfg.Path, interval),
		})
	}
	return runs
}

func drainExtraProbeRuns(runs []extraProbeRun) []extraProbeStats {
	stats := make([]extraProbeStats, 0, len(runs))
	for _, run := range runs {
		if run.run.done == nil {
			continue
		}
		stats = append(stats, extraProbeStats{config: run.config, stats: <-run.run.done})
	}
	return stats
}

func startContinuousMutationWithBody(ctx context.Context, prober *routerHTTPProber, atespace, actor, path string, interval time.Duration, body []byte) continuousProbeRun {
	var seq atomic.Int64
	return startContinuousProbeWithDo(ctx, func(ctx context.Context) (httpProbeResult, error) {
		mutationID := fmt.Sprintf("%s-continuous-mutation-%06d", actor, seq.Add(1))
		result, err := prober.DoWithHeadersAndBody(ctx, http.MethodPost, atespace, actor, path, map[string]string{"Idempotency-Key": mutationID}, body)
		result.IdempotencyKey = mutationID
		return result, err
	}, interval)
}

func mutationBody(size int64) []byte {
	if size <= 0 {
		return nil
	}
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i & 0xff)
	}
	return body
}

func startContinuousProbeWithDo(ctx context.Context, probe func(context.Context) (httpProbeResult, error), interval time.Duration) continuousProbeRun {
	done := make(chan continuousProbeStats, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var (
			mu             sync.Mutex
			wg             sync.WaitGroup
			samples        []continuousProbeSample
			nextSeq        int
			stopObservedAt time.Time
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
					sequence:       seq,
					start:          start,
					end:            time.Now(),
					result:         result,
					err:            err,
					canceledByStop: isProbeCanceledByStop(ctx, err),
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
				stopObservedAt = time.Now()
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
		var lastSuccessStart time.Time
		var lastSuccessEnd time.Time
		var completedSuccesses []continuousProbeSample
		for _, sample := range samples {
			if sample.canceledByStop {
				stats.canceledByStop++
				if !stopObservedAt.IsZero() && sample.start.Before(stopObservedAt) {
					stats.canceledStartedBeforeStop++
					if sample.end.After(stopObservedAt) {
						stats.inflightAtStopCount++
						if age := stopObservedAt.Sub(sample.start); age > stats.oldestInflightAgeAtStop {
							stats.oldestInflightAgeAtStop = age
						}
					}
				}
				continue
			}
			stats.requests++
			statusKey := "error"
			if sample.result.StatusCode > 0 {
				statusKey = strconv.Itoa(sample.result.StatusCode)
			}
			stats.statusCounts[statusKey]++
			if sample.err == nil && sample.result.StatusCode == http.StatusOK {
				stats.successes++
				if !hasStateProof(sample.result) {
					stats.missingStateProofCount++
				}
				if !hasRouteProof(sample.result) {
					stats.missingRouteProofCount++
				}
				latency := sample.end.Sub(sample.start)
				if latency > stats.maxSingleRequestLatency {
					stats.maxSingleRequestLatency = latency
				}
				if !lastSuccessStart.IsZero() {
					if gap := sample.start.Sub(lastSuccessStart); gap > stats.longestSuccessGap {
						stats.longestSuccessGap = gap
					}
					if gap := sample.start.Sub(lastSuccessStart); gap > stats.maxRequestStartGap {
						stats.maxRequestStartGap = gap
					}
					if gap := sample.end.Sub(lastSuccessEnd); gap > stats.maxSuccessCompletionGap {
						stats.maxSuccessCompletionGap = gap
					}
				}
				for _, prior := range completedSuccesses {
					if !prior.end.After(sample.start) && stateRegressed(prior.result, sample.result) {
						stats.monotonicViolationCount++
						break
					}
				}
				lastSuccessStart = sample.start
				lastSuccessEnd = sample.end
				completedSuccesses = append(completedSuccesses, sample)
			} else {
				stats.failures++
			}
		}
		done <- stats
	}()
	return continuousProbeRun{done: done}
}

func waitForCleanupIsolationWindow(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func observationEnd(targetSuccessEnd, cleanupObservationEnd time.Time) time.Time {
	if cleanupObservationEnd.After(targetSuccessEnd) {
		return cleanupObservationEnd
	}
	return targetSuccessEnd
}

func postSwitchIsolationStats(stats continuousProbeStats, targetSuccess time.Time) postSwitchIsolationProbeStats {
	var out postSwitchIsolationProbeStats
	for _, sample := range stats.samples {
		if sample.start.Before(targetSuccess) {
			continue
		}
		if sample.canceledByStop {
			continue
		}
		out.Requests++
		if sample.err == nil && sample.result.StatusCode == http.StatusOK {
			out.Successes++
			continue
		}
		out.ServiceErrors++
	}
	return out
}

func probeStatsInWindow(stats continuousProbeStats, start, end time.Time) postSwitchIsolationProbeStats {
	var out postSwitchIsolationProbeStats
	for _, sample := range stats.samples {
		if sample.start.Before(start) {
			continue
		}
		if !end.IsZero() && sample.start.After(end) {
			continue
		}
		if sample.canceledByStop {
			continue
		}
		out.Requests++
		if sample.err == nil && sample.result.StatusCode == http.StatusOK {
			out.Successes++
			continue
		}
		out.ServiceErrors++
	}
	return out
}

func routeSwitchedIsolationStats(stats continuousProbeStats, target workerPlacement) routeSwitchedIsolationProbeStats {
	var out routeSwitchedIsolationProbeStats
	targetKey := target.key()
	for _, sample := range stats.samples {
		if sample.result.RoutePhase != "PHASE_SWITCHED" {
			continue
		}
		if sample.canceledByStop {
			continue
		}
		out.Requests++
		if sample.err == nil && sample.result.StatusCode == http.StatusOK {
			out.Successes++
		} else {
			out.ServiceErrors++
		}
		if !hasRouteProof(sample.result) {
			out.MissingRouteProof++
		}
		if targetKey != "" && sample.result.RouteTargetWorker != "" && sample.result.RouteTargetWorker != targetKey {
			out.TargetMismatches++
			continue
		}
		if target.node != "" && sample.result.RouteTargetNode != "" && sample.result.RouteTargetNode != target.node {
			out.TargetMismatches++
		}
	}
	return out
}

func validateRouteSwitchedIsolationStats(stats routeSwitchedIsolationProbeStats, requireSamples bool) error {
	return validateRouteSwitchedIsolationStatsWithPrefix(stats, requireSamples, "")
}

func validateMutationRouteSwitchedIsolationStats(stats routeSwitchedIsolationProbeStats, requireSamples bool) error {
	return validateRouteSwitchedIsolationStatsWithPrefix(stats, requireSamples, "mutation_")
}

func validateRouteSwitchedIsolationStatsWithPrefix(stats routeSwitchedIsolationProbeStats, requireSamples bool, prefix string) error {
	var violations []string
	if requireSamples && stats.Requests == 0 {
		violations = append(violations, fmt.Sprintf("%sroute_switched_probe_requests=0", prefix))
	}
	if stats.ServiceErrors > 0 {
		violations = append(violations, fmt.Sprintf("%sservice_errors_during_route_switched=%d", prefix, stats.ServiceErrors))
	}
	if stats.TargetMismatches > 0 {
		violations = append(violations, fmt.Sprintf("%sroute_target_mismatch_during_route_switched=%d", prefix, stats.TargetMismatches))
	}
	if stats.MissingRouteProof > 0 {
		violations = append(violations, fmt.Sprintf("%smissing_route_proof_during_route_switched=%d", prefix, stats.MissingRouteProof))
	}
	if len(violations) == 0 {
		return nil
	}
	return status.Errorf(codes.FailedPrecondition, "route switched isolation violation: %s", strings.Join(violations, ", "))
}

func isProbeCanceledByStop(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	return errors.Is(err, context.Canceled)
}

func continuousProbeSampleEvent(runID, stage, workload, round, actorID, actor string, sampleIndex, nodeScale int, sample continuousProbeSample) perfdata.LifecycleEvent {
	ev := perfdata.LifecycleEvent{
		RunID:                 runID,
		OperationID:           fmt.Sprintf("%s-%03d-continuous-probe-%04d", actor, sampleIndex, sample.sequence),
		ActorID:               actorID,
		Operation:             "continuous_probe_sample",
		Stage:                 stage,
		StartTS:               sample.start.UTC().Format(time.RFC3339Nano),
		EndTS:                 sample.end.UTC().Format(time.RFC3339Nano),
		DurationMS:            sample.end.Sub(sample.start).Milliseconds(),
		Workload:              workload,
		Round:                 round,
		NodeScale:             nodeScale,
		Concurrency:           1,
		Result:                "ok",
		HTTPStatus:            sample.result.StatusCode,
		HTTPChecksum:          sample.result.Checksum,
		HTTPBodyBytes:         sample.result.BodyBytes,
		HTTPIdempotencyKey:    sample.result.IdempotencyKey,
		HTTPCounter:           sample.result.Counter,
		HTTPStateVersion:      sample.result.StateVersion,
		HTTPLastMutationID:    sample.result.LastMutationID,
		HTTPInstanceID:        sample.result.InstanceID,
		HTTPBootID:            sample.result.BootID,
		HTTPPodName:           sample.result.PodName,
		HTTPNodeName:          sample.result.NodeName,
		HTTPRouteGeneration:   sample.result.RouteGeneration,
		HTTPRoutePhase:        sample.result.RoutePhase,
		HTTPRouteTargetWorker: sample.result.RouteTargetWorker,
		HTTPRouteTargetNode:   sample.result.RouteTargetNode,
		HTTPRouteTargetIP:     sample.result.RouteTargetIP,
		HTTPBody:              sample.result.Body,
		HTTPStale:             sample.result.Stale,
		ProbeSequence:         sample.sequence,
	}
	if sample.err != nil {
		ev.Result = "error"
		ev.ErrorCode = status.Code(sample.err).String()
		ev.ErrorReason = sample.err.Error()
	}
	return ev
}

func continuousMutationSampleEvent(runID, stage, workload, round, actorID, actor string, sampleIndex, nodeScale int, sample continuousProbeSample) perfdata.LifecycleEvent {
	ev := continuousProbeSampleEvent(runID, stage, workload, round, actorID, actor, sampleIndex, nodeScale, sample)
	ev.OperationID = fmt.Sprintf("%s-%03d-continuous-mutation-%04d", actor, sampleIndex, sample.sequence)
	ev.Operation = "continuous_mutation_sample"
	return ev
}

func continuousExtraProbeSampleEvent(runID, stage, workload, round, actorID, actor string, sampleIndex, nodeScale int, extra extraProbeConfig, sample continuousProbeSample) perfdata.LifecycleEvent {
	ev := continuousProbeSampleEvent(runID, stage, workload, round, actorID, actor, sampleIndex, nodeScale, sample)
	ev.OperationID = fmt.Sprintf("%s-%03d-continuous-extra-%s-%04d", actor, sampleIndex, sanitizeName(extra.Label), sample.sequence)
	ev.Operation = "continuous_extra_probe_sample"
	ev.ProbeLabel = extra.Label
	ev.ProbePath = extra.Path
	return ev
}

func continuousExtraProbeSummaryEvent(runID, stage, workload, round, actorID, actor string, sampleIndex, nodeScale int, extra extraProbeConfig, stats continuousProbeStats, err error) perfdata.LifecycleEvent {
	ev := perfdata.LifecycleEvent{
		RunID:                          runID,
		OperationID:                    fmt.Sprintf("%s-%03d-continuous-extra-%s-summary", actor, sampleIndex, sanitizeName(extra.Label)),
		ActorID:                        actorID,
		Operation:                      "continuous_extra_probe_summary",
		Stage:                          stage,
		Workload:                       workload,
		Round:                          round,
		NodeScale:                      nodeScale,
		Concurrency:                    1,
		Result:                         "ok",
		ProbeLabel:                     extra.Label,
		ProbePath:                      extra.Path,
		LongestSuccessGapMS:            stats.longestSuccessGap.Milliseconds(),
		MaxRequestStartGapMS:           stats.maxRequestStartGap.Milliseconds(),
		MaxSuccessCompletionGapMS:      stats.maxSuccessCompletionGap.Milliseconds(),
		MaxSingleRequestLatencyMS:      stats.maxSingleRequestLatency.Milliseconds(),
		ProbeRequests:                  stats.requests,
		ProbeSuccesses:                 stats.successes,
		ProbeFailures:                  stats.failures,
		ProbeStatusCounts:              stats.statusCounts,
		ProbeCanceledByStop:            stats.canceledByStop,
		ProbeCanceledStartedBeforeStop: stats.canceledStartedBeforeStop,
		ProbeInflightAtStopCount:       stats.inflightAtStopCount,
		ProbeOldestInflightAgeAtStopMS: stats.oldestInflightAgeAtStop.Milliseconds(),
		MonotonicViolationCount:        stats.monotonicViolationCount,
		MissingStateProofCount:         stats.missingStateProofCount,
		MissingRouteProofCount:         stats.missingRouteProofCount,
	}
	if err != nil {
		ev.Result = "error"
		ev.ErrorCode = status.Code(err).String()
		ev.ErrorReason = err.Error()
	}
	return ev
}

func countStaleProbeVersions(samples []continuousProbeSample, baseline httpProbeResult) int {
	stale := 0
	for _, sample := range samples {
		if sample.canceledByStop || sample.err != nil || sample.result.StatusCode != http.StatusOK {
			continue
		}
		if sample.result.Counter < baseline.Counter {
			stale++
			continue
		}
		if baseline.StateVersion > 0 && sample.result.StateVersion < baseline.StateVersion {
			stale++
		}
	}
	return stale
}

func countDuplicateSuccessCounters(samples []continuousProbeSample) int {
	seen := map[int64]bool{}
	duplicates := 0
	for _, sample := range samples {
		if sample.canceledByStop || sample.err != nil || sample.result.StatusCode != http.StatusOK {
			continue
		}
		if sample.result.Counter <= 0 {
			continue
		}
		if seen[sample.result.Counter] {
			duplicates++
			continue
		}
		seen[sample.result.Counter] = true
	}
	return duplicates
}

func maxSuccessCounter(samples []continuousProbeSample) int64 {
	var maxCounter int64
	for _, sample := range samples {
		if sample.canceledByStop || sample.err != nil || sample.result.StatusCode != http.StatusOK {
			continue
		}
		if sample.result.Counter > maxCounter {
			maxCounter = sample.result.Counter
		}
	}
	return maxCounter
}

func countLostAcceptedMutations(samples []continuousProbeSample, final httpProbeResult) int {
	lost := 0
	for _, sample := range samples {
		if sample.canceledByStop || sample.err != nil || sample.result.StatusCode != http.StatusOK {
			continue
		}
		if sample.result.Counter > final.Counter {
			lost++
		}
	}
	return lost
}

func dualActiveObserved(samples []continuousProbeSample) bool {
	instances := map[string]bool{}
	for _, sample := range samples {
		if sample.canceledByStop || sample.err != nil || sample.result.StatusCode != http.StatusOK {
			continue
		}
		key := sample.result.BootID
		if key == "" {
			key = sample.result.InstanceID
		}
		if key != "" {
			instances[key] = true
		}
	}
	return len(instances) > 1
}

type sseProbeRun struct {
	ready <-chan error
	done  <-chan sseProbeStats
}

type sseProbeStats struct {
	events                int
	startedBeforePrepare  bool
	eventsBeforePrepare   int
	disconnects           int
	maxMessageGap         time.Duration
	maxMessageGapPrevious sseFrameSample
	maxMessageGapCurrent  sseFrameSample
	gapThresholdViolation bool
	sequenceGapCount      int
	duplicateCount        int
	stateRegressionCount  int
	err                   error
}

type sseStateFrame struct {
	Sequence       int64  `json:"sequence"`
	StateVersion   int64  `json:"state_version"`
	Counter        int64  `json:"counter"`
	Checksum       string `json:"checksum"`
	LastMutationID string `json:"last_mutation_id"`
	BootID         string `json:"boot_id"`
	InstanceID     string `json:"instance_id"`
	PodName        string `json:"pod_name"`
	NodeName       string `json:"node_name"`
	ServerUnixNano int64  `json:"server_unix_nano"`
}

type sseFrameSample struct {
	receivedAt time.Time
	frame      sseStateFrame
}

func startSSEProbe(ctx context.Context, prober *routerHTTPProber, atespace, actor, path string) sseProbeRun {
	ready := make(chan error, 1)
	done := make(chan sseProbeStats, 1)
	go func() {
		defer close(done)
		resp, err := prober.Stream(ctx, atespace, actor, path)
		if err != nil {
			ready <- err
			stats := sseProbeStats{err: err}
			if ctx.Err() == nil {
				stats.disconnects = 1
			}
			done <- stats
			return
		}
		defer resp.Body.Close()
		done <- readSSEProbeStats(ctx, resp.Body, ready)
	}()
	return sseProbeRun{ready: ready, done: done}
}

const sseMaxMessageGapThreshold = 3 * time.Second

func readSSEProbeStats(ctx context.Context, r io.Reader, ready chan<- error) sseProbeStats {
	stats := sseProbeStats{}
	readySent := false
	defer func() {
		if !readySent {
			ready <- fmt.Errorf("SSE ended before first event")
		}
	}()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var lastMessageAt time.Time
	var lastMessageFrame sseFrameSample
	var lastSequence int64
	var lastStateVersion int64
	seen := map[int64]bool{}
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		frame, err := parseSSEStateFrame(strings.TrimPrefix(line, "data: "))
		if err != nil {
			stats.err = err
			stats.disconnects++
			return stats
		}
		now := time.Now()
		if !lastMessageAt.IsZero() {
			if gap := now.Sub(lastMessageAt); gap > stats.maxMessageGap {
				stats.maxMessageGap = gap
				stats.maxMessageGapPrevious = lastMessageFrame
				stats.maxMessageGapCurrent = sseFrameSample{receivedAt: now, frame: frame}
			}
			if gap := now.Sub(lastMessageAt); gap > sseMaxMessageGapThreshold {
				stats.gapThresholdViolation = true
			}
		}
		lastMessageAt = now
		lastMessageFrame = sseFrameSample{receivedAt: now, frame: frame}
		stats.events++
		if !readySent {
			readySent = true
			ready <- nil
		}
		duplicate := seen[frame.Sequence]
		if duplicate {
			stats.duplicateCount++
		}
		seen[frame.Sequence] = true
		if !duplicate && lastSequence > 0 && frame.Sequence != lastSequence+1 {
			stats.sequenceGapCount++
		}
		if lastStateVersion > 0 && frame.StateVersion < lastStateVersion {
			stats.stateRegressionCount++
		}
		lastSequence = frame.Sequence
		lastStateVersion = frame.StateVersion
	}
	if err := scanner.Err(); err != nil {
		stats.err = err
		if ctx.Err() == nil {
			stats.disconnects++
		}
	} else if ctx.Err() == nil {
		stats.disconnects++
	}
	return stats
}

func parseSSEStateFrame(raw string) (sseStateFrame, error) {
	var frame sseStateFrame
	if err := json.Unmarshal([]byte(raw), &frame); err != nil {
		return frame, err
	}
	return frame, nil
}

func sseProbeSummaryEvent(cfg HotMigrationConfig, actor string, sampleIndex int, start, end time.Time, stats sseProbeStats) perfdata.LifecycleEvent {
	ev := perfdata.LifecycleEvent{
		RunID:                       cfg.RunID,
		OperationID:                 fmt.Sprintf("%s-%03d-sse-probe-summary", actor, sampleIndex),
		ActorID:                     cfg.Atespace + "/" + actor,
		Operation:                   "sse_probe_summary",
		Stage:                       cfg.Stage,
		StartTS:                     start.UTC().Format(time.RFC3339Nano),
		EndTS:                       end.UTC().Format(time.RFC3339Nano),
		DurationMS:                  end.Sub(start).Milliseconds(),
		Workload:                    cfg.Workload,
		Round:                       cfg.Round,
		NodeScale:                   cfg.NodeScale,
		Concurrency:                 cfg.Concurrency,
		Result:                      "ok",
		StreamEvents:                stats.events,
		StreamStartedBeforePrepare:  stats.startedBeforePrepare,
		StreamEventsBeforePrepare:   stats.eventsBeforePrepare,
		StreamDisconnects:           stats.disconnects,
		StreamMaxMessageGapMS:       stats.maxMessageGap.Milliseconds(),
		StreamGapThresholdViolation: stats.gapThresholdViolation,
		StreamSequenceGapCount:      stats.sequenceGapCount,
		StreamDuplicateCount:        stats.duplicateCount,
		StreamStateRegressionCount:  stats.stateRegressionCount,
		StreamMaxGapPreviousFrame:   sseFrameEvidence(start, stats.maxMessageGapPrevious),
		StreamMaxGapCurrentFrame:    sseFrameEvidence(start, stats.maxMessageGapCurrent),
	}
	if err := validateSSEProbeStats(stats); err != nil {
		ev.Result = "error"
		ev.ErrorReason = err.Error()
	}
	return ev
}

func validateSSEProbeStats(stats sseProbeStats) error {
	if stats.err != nil && !errors.Is(stats.err, context.Canceled) {
		return stats.err
	}
	var violations []string
	if stats.events == 0 {
		violations = append(violations, "events=0")
	}
	if !stats.startedBeforePrepare {
		violations = append(violations, "started_before_prepare=false")
	}
	if stats.disconnects > 0 {
		violations = append(violations, fmt.Sprintf("disconnects=%d", stats.disconnects))
	}
	if stats.gapThresholdViolation {
		violations = append(violations, fmt.Sprintf("max_message_gap_ms=%d threshold_ms=%d", stats.maxMessageGap.Milliseconds(), sseMaxMessageGapThreshold.Milliseconds()))
	}
	if stats.sequenceGapCount > 0 {
		violations = append(violations, fmt.Sprintf("sequence_gaps=%d", stats.sequenceGapCount))
	}
	if stats.duplicateCount > 0 {
		violations = append(violations, fmt.Sprintf("duplicates=%d", stats.duplicateCount))
	}
	if stats.stateRegressionCount > 0 {
		violations = append(violations, fmt.Sprintf("state_regressions=%d", stats.stateRegressionCount))
	}
	if len(violations) > 0 {
		return fmt.Errorf("SSE continuity violation: %s", strings.Join(violations, ", "))
	}
	return nil
}

type terminalWebSocketProbeRun struct {
	ready <-chan error
	done  <-chan terminalWebSocketStats
}

type terminalWebSocketStats struct {
	messages                 int
	startedBeforePrepare     bool
	ticks                    int
	echoes                   int
	expectedEchoes           int
	disconnects              int
	maxMessageGap            time.Duration
	maxMessageGapStart       time.Time
	maxMessageGapEnd         time.Time
	maxMessageGapPrevious    terminalFrameSample
	maxMessageGapCurrent     terminalFrameSample
	gapThresholdViolation    bool
	sequenceGapCount         int
	duplicateCount           int
	missingEchoCount         int
	missingEchoSequences     []int
	sentEchoAt               map[int]time.Time
	echoLatencies            []time.Duration
	maxEchoLatencySequence   int
	maxEchoLatencySentAt     time.Time
	maxEchoLatencyReceivedAt time.Time
	maxEchoLatencyFrame      terminalFrameSample
	pidChanged               bool
	stateRegressionCount     int
	stateRegressionSamples   []terminalStateRegressionSample
	err                      error
}

type terminalFrame struct {
	Type                     string `json:"type"`
	TerminalSequence         int64  `json:"terminal_sequence"`
	TerminalPID              int    `json:"terminal_pid"`
	Input                    string `json:"input"`
	StateVersion             int64  `json:"state_version"`
	Counter                  int64  `json:"counter"`
	BootID                   string `json:"boot_id"`
	InstanceID               string `json:"instance_id"`
	LastMutationID           string `json:"last_mutation_id"`
	ServerUnixNano           int64  `json:"server_unix_nano"`
	ServerReadUnixNano       int64  `json:"server_read_unix_nano"`
	ServerEnqueueUnixNano    int64  `json:"server_enqueue_unix_nano"`
	ServerWriteStartUnixNano int64  `json:"server_write_start_unix_nano"`
	ServerWriteEndUnixNano   int64  `json:"server_write_end_unix_nano"`
}

type terminalFrameSample struct {
	receivedAt time.Time
	frame      terminalFrame
}

type terminalStateRegressionSample struct {
	previous terminalFrameSample
	current  terminalFrameSample
}

func startTerminalWebSocketProbe(ctx context.Context, prober *routerHTTPProber, atespace, actor, path string, interval time.Duration) terminalWebSocketProbeRun {
	ready := make(chan error, 1)
	done := make(chan terminalWebSocketStats, 1)
	go func() {
		conn, err := prober.WebSocket(ctx, atespace, actor, path)
		if err != nil {
			ready <- err
			stats := terminalWebSocketStats{err: err}
			if ctx.Err() == nil {
				stats.disconnects = 1
			}
			done <- stats
			return
		}
		defer conn.Close()
		done <- readTerminalWebSocketStats(ctx, conn, ready, interval)
	}()
	return terminalWebSocketProbeRun{ready: ready, done: done}
}

const (
	terminalMaxMessageGapThreshold = 3 * time.Second
	terminalStopDrainTimeout       = 3 * time.Second
)

func readTerminalWebSocketStats(ctx context.Context, conn *websocket.Conn, ready chan<- error, interval time.Duration) terminalWebSocketStats {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	stats := terminalWebSocketStats{}
	readySent := false
	defer func() {
		if !readySent {
			ready <- fmt.Errorf("terminal websocket ended before first message")
		}
	}()
	var expectedEchoes atomic.Int64
	var sentMu sync.Mutex
	sentEchoAt := map[int]time.Time{}
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		seq := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				now := time.Now()
				msg := fmt.Sprintf("client-seq=%d client_unix_nano=%d", seq, now.UnixNano())
				_ = conn.SetWriteDeadline(now.Add(2 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
					return
				}
				sentMu.Lock()
				sentEchoAt[seq] = now
				sentMu.Unlock()
				expectedEchoes.Add(1)
			}
		}
	}()

	var lastMessageAt time.Time
	var lastMessageFrame terminalFrameSample
	var lastSequence int64
	var lastStateVersion int64
	var lastStateFrame terminalFrameSample
	var pid int
	seenSeq := map[int64]bool{}
	seenEcho := map[int]bool{}
	var stopAt time.Time
	for {
		now := time.Now()
		if ctx.Err() != nil {
			if stopAt.IsZero() {
				stopAt = now
			}
			if terminalAllExpectedEchoesSeen(int(expectedEchoes.Load()), seenEcho) || now.Sub(stopAt) > terminalStopDrainTimeout {
				break
			}
			_ = conn.SetReadDeadline(minTime(now.Add(200*time.Millisecond), stopAt.Add(terminalStopDrainTimeout)))
		} else {
			_ = conn.SetReadDeadline(now.Add(terminalMaxMessageGapThreshold))
		}
		var frame terminalFrame
		if err := conn.ReadJSON(&frame); err != nil {
			if ctx.Err() != nil {
				stats.err = ctx.Err()
			} else {
				stats.err = err
				stats.disconnects++
			}
			break
		}
		now = time.Now()
		if !lastMessageAt.IsZero() {
			if gap := now.Sub(lastMessageAt); gap > stats.maxMessageGap {
				stats.maxMessageGap = gap
				stats.maxMessageGapStart = lastMessageAt
				stats.maxMessageGapEnd = now
				stats.maxMessageGapPrevious = lastMessageFrame
				stats.maxMessageGapCurrent = terminalFrameSample{receivedAt: now, frame: frame}
			}
			if gap := now.Sub(lastMessageAt); gap > terminalMaxMessageGapThreshold {
				stats.gapThresholdViolation = true
			}
		}
		lastMessageAt = now
		lastMessageFrame = terminalFrameSample{receivedAt: now, frame: frame}
		stats.messages++
		if !readySent {
			readySent = true
			ready <- nil
		}
		if frame.TerminalPID > 0 {
			if pid == 0 {
				pid = frame.TerminalPID
			} else if pid != frame.TerminalPID {
				stats.pidChanged = true
			}
		}
		if frame.StateVersion > 0 && lastStateVersion > 0 && frame.StateVersion < lastStateVersion {
			stats.stateRegressionCount++
			if len(stats.stateRegressionSamples) < 5 {
				stats.stateRegressionSamples = append(stats.stateRegressionSamples, terminalStateRegressionSample{
					previous: lastStateFrame,
					current:  terminalFrameSample{receivedAt: now, frame: frame},
				})
			}
		}
		if frame.StateVersion > 0 {
			lastStateVersion = frame.StateVersion
			lastStateFrame = terminalFrameSample{receivedAt: now, frame: frame}
		}
		switch frame.Type {
		case "tick":
			stats.ticks++
			if frame.TerminalSequence > 0 {
				if seenSeq[frame.TerminalSequence] {
					stats.duplicateCount++
				}
				if lastSequence > 0 && frame.TerminalSequence != lastSequence+1 {
					stats.sequenceGapCount++
				}
				seenSeq[frame.TerminalSequence] = true
				lastSequence = frame.TerminalSequence
			}
		case "echo":
			stats.echoes++
			if seq, ok := parseTerminalClientSequence(frame.Input); ok {
				seenEcho[seq] = true
				sentMu.Lock()
				sentAt := sentEchoAt[seq]
				sentMu.Unlock()
				if !sentAt.IsZero() {
					latency := now.Sub(sentAt)
					stats.echoLatencies = append(stats.echoLatencies, latency)
					if stats.maxEchoLatencyReceivedAt.IsZero() || latency > stats.maxEchoLatencyReceivedAt.Sub(stats.maxEchoLatencySentAt) {
						stats.maxEchoLatencySequence = seq
						stats.maxEchoLatencySentAt = sentAt
						stats.maxEchoLatencyReceivedAt = now
						stats.maxEchoLatencyFrame = terminalFrameSample{receivedAt: now, frame: frame}
					}
				}
			}
		}
	}
	<-writeDone
	stats.expectedEchoes = int(expectedEchoes.Load())
	for i := 1; i <= stats.expectedEchoes; i++ {
		if !seenEcho[i] {
			stats.missingEchoCount++
			stats.missingEchoSequences = append(stats.missingEchoSequences, i)
		}
	}
	sentMu.Lock()
	stats.sentEchoAt = maps.Clone(sentEchoAt)
	sentMu.Unlock()
	return stats
}

func terminalAllExpectedEchoesSeen(expected int, seen map[int]bool) bool {
	for i := 1; i <= expected; i++ {
		if !seen[i] {
			return false
		}
	}
	return true
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func parseTerminalClientSequence(input string) (int, bool) {
	const prefix = "client-seq="
	if !strings.HasPrefix(input, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(input, prefix)
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	seq, err := strconv.Atoi(rest[:end])
	if err != nil || seq <= 0 {
		return 0, false
	}
	return seq, true
}

func parseTerminalClientUnixNano(input string) int64 {
	const key = "client_unix_nano="
	idx := strings.Index(input, key)
	if idx < 0 {
		return 0
	}
	rest := input[idx+len(key):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0
	}
	value, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func terminalWebSocketSummaryEvent(cfg HotMigrationConfig, actor string, sampleIndex int, start, end time.Time, stats terminalWebSocketStats) perfdata.LifecycleEvent {
	missingOffsets := terminalMissingEchoSentOffsets(start, stats)
	echoLatencyMax, echoLatencyP95 := terminalEchoLatencyStats(stats)
	ev := perfdata.LifecycleEvent{
		RunID:                                  cfg.RunID,
		OperationID:                            fmt.Sprintf("%s-%03d-terminal-websocket-summary", actor, sampleIndex),
		ActorID:                                cfg.Atespace + "/" + actor,
		Operation:                              "terminal_websocket_probe_summary",
		Stage:                                  cfg.Stage,
		StartTS:                                start.UTC().Format(time.RFC3339Nano),
		EndTS:                                  end.UTC().Format(time.RFC3339Nano),
		DurationMS:                             end.Sub(start).Milliseconds(),
		Workload:                               cfg.Workload,
		Round:                                  cfg.Round,
		NodeScale:                              cfg.NodeScale,
		Concurrency:                            cfg.Concurrency,
		Result:                                 "ok",
		TerminalMessages:                       stats.messages,
		TerminalStartedBeforePrepare:           stats.startedBeforePrepare,
		TerminalTicks:                          stats.ticks,
		TerminalEchoes:                         stats.echoes,
		TerminalExpectedEchoes:                 stats.expectedEchoes,
		TerminalDisconnects:                    stats.disconnects,
		TerminalReconnectsObserved:             stats.disconnects,
		TerminalUXGapObserved:                  terminalUXGapObserved(stats),
		TerminalMaxMessageGapMS:                stats.maxMessageGap.Milliseconds(),
		TerminalMaxMessageGapStartOffsetMS:     terminalOffsetMS(start, stats.maxMessageGapStart),
		TerminalMaxMessageGapEndOffsetMS:       terminalOffsetMS(start, stats.maxMessageGapEnd),
		TerminalEchoLatencyMaxMS:               echoLatencyMax.Milliseconds(),
		TerminalEchoLatencyP95MS:               echoLatencyP95.Milliseconds(),
		TerminalEchoLatencyMaxSequence:         stats.maxEchoLatencySequence,
		TerminalEchoLatencyMaxSentOffsetMS:     terminalOffsetMS(start, stats.maxEchoLatencySentAt),
		TerminalEchoLatencyMaxReceivedOffsetMS: terminalOffsetMS(start, stats.maxEchoLatencyReceivedAt),
		TerminalGapThresholdViolation:          stats.gapThresholdViolation,
		TerminalSequenceGapCount:               stats.sequenceGapCount,
		TerminalDuplicateCount:                 stats.duplicateCount,
		TerminalMissingEchoCount:               stats.missingEchoCount,
		TerminalMissingEchoSequences:           stats.missingEchoSequences,
		TerminalMissingEchoSentOffsetsMS:       missingOffsets,
		TerminalPIDChanged:                     stats.pidChanged,
		TerminalStateRegressionCount:           stats.stateRegressionCount,
		TerminalContinuityResult:               terminalContinuityResult(stats),
		TerminalUXResult:                       terminalUXResult(stats),
		TerminalMaxGapPreviousFrame:            terminalFrameEvidence(start, stats.maxMessageGapPrevious),
		TerminalMaxGapCurrentFrame:             terminalFrameEvidence(start, stats.maxMessageGapCurrent),
		TerminalEchoLatencyMaxFrame:            terminalFrameEvidence(start, stats.maxEchoLatencyFrame),
		TerminalStateRegressionSamples:         terminalStateRegressionEvidence(start, stats.stateRegressionSamples),
	}
	if len(stats.missingEchoSequences) > 0 {
		ev.TerminalFirstMissingEchoSequence = stats.missingEchoSequences[0]
		ev.TerminalLastMissingEchoSequence = stats.missingEchoSequences[len(stats.missingEchoSequences)-1]
	}
	if err := validateTerminalWebSocketStats(stats); err != nil {
		ev.Result = "error"
		ev.ErrorReason = err.Error()
	}
	return ev
}

func terminalFrameEvidence(start time.Time, sample terminalFrameSample) *perfdata.TerminalFrameEvidence {
	if sample.receivedAt.IsZero() {
		return nil
	}
	clientSeq, _ := parseTerminalClientSequence(sample.frame.Input)
	return &perfdata.TerminalFrameEvidence{
		ReceivedOffsetMS:         sample.receivedAt.Sub(start).Milliseconds(),
		Type:                     sample.frame.Type,
		TerminalSequence:         sample.frame.TerminalSequence,
		Input:                    sample.frame.Input,
		ClientSequence:           clientSeq,
		Counter:                  sample.frame.Counter,
		StateVersion:             sample.frame.StateVersion,
		LastMutationID:           sample.frame.LastMutationID,
		BootID:                   sample.frame.BootID,
		InstanceID:               sample.frame.InstanceID,
		TerminalPID:              sample.frame.TerminalPID,
		ClientUnixNano:           parseTerminalClientUnixNano(sample.frame.Input),
		ServerUnixNano:           sample.frame.ServerUnixNano,
		ServerReadUnixNano:       sample.frame.ServerReadUnixNano,
		ServerEnqueueUnixNano:    sample.frame.ServerEnqueueUnixNano,
		ServerWriteStartUnixNano: sample.frame.ServerWriteStartUnixNano,
		ServerWriteEndUnixNano:   sample.frame.ServerWriteEndUnixNano,
	}
}

func sseFrameEvidence(start time.Time, sample sseFrameSample) *perfdata.StreamFrameEvidence {
	if sample.receivedAt.IsZero() {
		return nil
	}
	return &perfdata.StreamFrameEvidence{
		ReceivedOffsetMS: sample.receivedAt.Sub(start).Milliseconds(),
		Sequence:         sample.frame.Sequence,
		Counter:          sample.frame.Counter,
		StateVersion:     sample.frame.StateVersion,
		Checksum:         sample.frame.Checksum,
		LastMutationID:   sample.frame.LastMutationID,
		BootID:           sample.frame.BootID,
		InstanceID:       sample.frame.InstanceID,
		PodName:          sample.frame.PodName,
		NodeName:         sample.frame.NodeName,
		ServerUnixNano:   sample.frame.ServerUnixNano,
	}
}

func terminalStateRegressionEvidence(start time.Time, samples []terminalStateRegressionSample) []perfdata.TerminalStateRegressionEvidence {
	if len(samples) == 0 {
		return nil
	}
	out := make([]perfdata.TerminalStateRegressionEvidence, 0, len(samples))
	for _, sample := range samples {
		previous := terminalFrameEvidence(start, sample.previous)
		current := terminalFrameEvidence(start, sample.current)
		if previous == nil || current == nil {
			continue
		}
		out = append(out, perfdata.TerminalStateRegressionEvidence{
			Previous: *previous,
			Current:  *current,
		})
	}
	return out
}

func terminalOffsetMS(start, t time.Time) int64 {
	if start.IsZero() || t.IsZero() {
		return 0
	}
	return t.Sub(start).Milliseconds()
}

func terminalMissingEchoSentOffsets(start time.Time, stats terminalWebSocketStats) []int64 {
	if len(stats.missingEchoSequences) == 0 || len(stats.sentEchoAt) == 0 {
		return nil
	}
	offsets := make([]int64, 0, len(stats.missingEchoSequences))
	for _, seq := range stats.missingEchoSequences {
		sentAt, ok := stats.sentEchoAt[seq]
		if !ok || sentAt.IsZero() {
			continue
		}
		offsets = append(offsets, sentAt.Sub(start).Milliseconds())
	}
	return offsets
}

func terminalEchoLatencyStats(stats terminalWebSocketStats) (time.Duration, time.Duration) {
	if len(stats.echoLatencies) == 0 {
		return 0, 0
	}
	latencies := append([]time.Duration(nil), stats.echoLatencies...)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	max := latencies[len(latencies)-1]
	p95Index := int(math.Ceil(float64(len(latencies))*0.95)) - 1
	if p95Index < 0 {
		p95Index = 0
	}
	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	return max, latencies[p95Index]
}

func terminalContinuityResult(stats terminalWebSocketStats) string {
	if err := validateTerminalWebSocketStats(stats); err != nil {
		return "error"
	}
	return "ok"
}

const terminalUXGapTarget = time.Second

func terminalUXGapObserved(stats terminalWebSocketStats) bool {
	echoMax, _ := terminalEchoLatencyStats(stats)
	return stats.maxMessageGap >= terminalUXGapTarget || echoMax >= terminalUXGapTarget
}

func terminalUXResult(stats terminalWebSocketStats) string {
	if terminalContinuityResult(stats) != "ok" {
		return "error"
	}
	if terminalUXGapObserved(stats) {
		return "degraded"
	}
	return "ok"
}

func validateTerminalWebSocketStats(stats terminalWebSocketStats) error {
	if stats.err != nil && !errors.Is(stats.err, context.Canceled) && !websocket.IsCloseError(stats.err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		return stats.err
	}
	var violations []string
	if stats.messages == 0 {
		violations = append(violations, "messages=0")
	}
	if !stats.startedBeforePrepare {
		violations = append(violations, "started_before_prepare=false")
	}
	if stats.disconnects > 0 {
		violations = append(violations, fmt.Sprintf("disconnects=%d", stats.disconnects))
	}
	if stats.gapThresholdViolation {
		violations = append(violations, fmt.Sprintf("max_message_gap_ms=%d threshold_ms=%d", stats.maxMessageGap.Milliseconds(), terminalMaxMessageGapThreshold.Milliseconds()))
	}
	if stats.sequenceGapCount > 0 {
		violations = append(violations, fmt.Sprintf("sequence_gaps=%d", stats.sequenceGapCount))
	}
	if stats.duplicateCount > 0 {
		violations = append(violations, fmt.Sprintf("duplicates=%d", stats.duplicateCount))
	}
	if stats.missingEchoCount > 0 {
		violations = append(violations, fmt.Sprintf("missing_echoes=%d", stats.missingEchoCount))
	}
	if stats.pidChanged {
		violations = append(violations, "pid_changed=true")
	}
	if stats.stateRegressionCount > 0 {
		violations = append(violations, fmt.Sprintf("state_regressions=%d", stats.stateRegressionCount))
	}
	if len(violations) > 0 {
		return fmt.Errorf("terminal websocket continuity violation: %s", strings.Join(violations, ", "))
	}
	return nil
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
