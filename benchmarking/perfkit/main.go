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

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/benchmarking/perfkit/internal/perfdata"
	"github.com/agent-substrate/substrate/benchmarking/perfkit/internal/preflight"
	"github.com/agent-substrate/substrate/benchmarking/perfkit/internal/runner"
	"github.com/agent-substrate/substrate/internal/ateclient"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "preflight":
		err = runPreflight(os.Args[2:])
	case "prepare-ack-nodes":
		err = runPrepareACKNodes(os.Args[2:])
	case "report":
		err = runReport(os.Args[2:])
	case "run-lifecycle":
		err = runLifecycle(os.Args[2:])
	case "run-cross-node-recover":
		err = runCrossNodeRecover(os.Args[2:])
	case "run-hot-migration":
		err = runHotMigration(os.Args[2:])
	case "cleanup-atespace":
		err = runCleanup(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "perfkit:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: go run ./benchmarking/perfkit <command> [flags]

commands:
  preflight          check whether a Kubernetes cluster is ready for perfkit/Substrate tests
  prepare-ack-nodes  prepare ACK nodes for gVisor worker validation
  report             generate derived CSV, summary JSON, HTML report, and manifest
  run-lifecycle      run create/resume/suspend/resume/suspend/delete lifecycle workload
  run-cross-node-recover run external-snapshot recover and require target node != source node
  run-hot-migration  run request-level cross-node hot migration with continuous HTTP probes
  cleanup-atespace   suspend/delete residual actors and delete an empty atespace`)
}

func runPreflight(args []string) error {
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path")
	outPath := fs.String("out", "", "output preflight JSON path")
	requireSubstrate := fs.Bool("require-substrate", true, "require ate-system and core Substrate pods")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kubeconfig == "" || *outPath == "" {
		return fmt.Errorf("--kubeconfig and --out are required")
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	obs, err := preflight.Collect(ctx, *kubeconfig, *requireSubstrate)
	if err != nil {
		obs, err = preflight.CollectWithKubectl(ctx, *kubeconfig, *requireSubstrate)
		if err != nil {
			return err
		}
	}
	result := preflight.Evaluate(obs)
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(*outPath, data, 0o644); err != nil {
		return err
	}
	fmt.Printf("preflight %s: %s\n", result.Status, *outPath)
	if result.Status == preflight.StatusFail {
		return fmt.Errorf("preflight failed")
	}
	return nil
}

type nodePrepEvent struct {
	Timestamp string `json:"timestamp"`
	Node      string `json:"node"`
	Pod       string `json:"pod"`
	Sysctl    string `json:"sysctl"`
	Desired   int    `json:"desired"`
	Status    string `json:"status"`
	Output    string `json:"output,omitempty"`
	Error     string `json:"error,omitempty"`
}

func runPrepareACKNodes(args []string) error {
	fs := flag.NewFlagSet("prepare-ack-nodes", flag.ExitOnError)
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path")
	outPath := fs.String("out", "", "output JSONL path")
	namespace := fs.String("namespace", "default", "namespace for temporary node preparation pods")
	debugImage := fs.String("debug-image", "busybox:1.36", "debug image with sh and chroot; use an ACK/VPC mirror when public pulls are blocked")
	userMaxNamespaces := fs.Int("user-max-user-namespaces", 28633, "value for user.max_user_namespaces")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kubeconfig == "" || *outPath == "" {
		return fmt.Errorf("--kubeconfig and --out are required")
	}
	if *userMaxNamespaces <= 0 {
		return fmt.Errorf("--user-max-user-namespaces must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	nodesOut, err := kubectlCombined(ctx, *kubeconfig, "get", "nodes", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return fmt.Errorf("list nodes: %w: %s", err, strings.TrimSpace(nodesOut))
	}
	nodes := strings.Fields(nodesOut)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes found")
	}

	f, err := os.Create(*outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	var failed []string
	for _, node := range nodes {
		pod := "perfkit-nodeprep-" + sanitizePodName(node) + "-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 36)
		event := nodePrepEvent{
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Node:      node,
			Pod:       pod,
			Sysctl:    "user.max_user_namespaces",
			Desired:   *userMaxNamespaces,
			Status:    "ok",
		}
		output, prepErr := prepareNodeSysctl(ctx, *kubeconfig, *namespace, pod, node, *debugImage, *userMaxNamespaces)
		event.Output = output
		if prepErr != nil {
			event.Status = "error"
			event.Error = prepErr.Error()
			failed = append(failed, node)
		}
		if err := enc.Encode(event); err != nil {
			return err
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("prepare ACK nodes failed for: %s", strings.Join(failed, ", "))
	}
	fmt.Printf("prepared %d ACK nodes: %s\n", len(nodes), *outPath)
	return nil
}

func prepareNodeSysctl(ctx context.Context, kubeconfig, namespace, pod, node, image string, value int) (string, error) {
	command := fmt.Sprintf(`set -eu
before=$(cat /proc/sys/user/max_user_namespaces)
sysctl -w user.max_user_namespaces=%d
after=$(cat /proc/sys/user/max_user_namespaces)
echo before=$before after=$after
test "$after" = "%d"`, value, value)
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/name: substrate-perfkit-nodeprep
spec:
  restartPolicy: Never
  nodeName: %s
  hostPID: true
  tolerations:
  - operator: Exists
  containers:
  - name: nodeprep
    image: %s
    command: ["chroot", "/host", "sh", "-c", %q]
    securityContext:
      privileged: true
      runAsUser: 0
      runAsGroup: 0
    volumeMounts:
    - name: host-root
      mountPath: /host
  volumes:
  - name: host-root
    hostPath:
      path: /
      type: Directory
`, pod, namespace, node, image, command)

	var output strings.Builder
	out, err := kubectlCombinedInput(ctx, kubeconfig, manifest, "apply", "-f", "-")
	output.WriteString(out)
	if err != nil {
		return output.String(), fmt.Errorf("create nodeprep pod: %w", err)
	}
	waitOut, waitErr := kubectlCombined(ctx, kubeconfig, "-n", namespace, "wait", "--for=jsonpath={.status.phase}=Succeeded", "pod/"+pod, "--timeout=120s")
	output.WriteString(waitOut)
	logOut, logErr := kubectlCombined(ctx, kubeconfig, "-n", namespace, "logs", pod)
	output.WriteString(logOut)
	deleteOut, _ := kubectlCombined(ctx, kubeconfig, "-n", namespace, "delete", "pod", pod, "--ignore-not-found=true")
	output.WriteString(deleteOut)
	if waitErr != nil {
		return output.String(), fmt.Errorf("wait for nodeprep pod: %w", waitErr)
	}
	if logErr != nil {
		return output.String(), fmt.Errorf("read nodeprep logs: %w", logErr)
	}
	return output.String(), nil
}

func sanitizePodName(value string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(value) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "node"
	}
	if len(out) > 40 {
		out = strings.Trim(out[:40], "-")
	}
	return out
}

func kubectlCombined(ctx context.Context, kubeconfig string, args ...string) (string, error) {
	return kubectlCombinedInput(ctx, kubeconfig, "", args...)
}

func kubectlCombinedInput(ctx context.Context, kubeconfig, stdin string, args ...string) (string, error) {
	fullArgs := append([]string{"--kubeconfig", kubeconfig}, args...)
	cmd := exec.CommandContext(ctx, "kubectl", fullArgs...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func runReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	runDir := fs.String("run-dir", "", "run directory")
	runID := fs.String("run-id", "", "run id for report metadata")
	eventFiles := fs.String("event-files", "", "comma-separated lifecycle JSONL files; defaults to raw/substrate-api/lifecycle-events*.jsonl")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return fmt.Errorf("--run-dir is required")
	}
	files, err := lifecycleFiles(*runDir, *eventFiles)
	if err != nil {
		return err
	}
	var events []perfdata.LifecycleEvent
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defaults := defaultsForFile(file)
		rows, readErr := perfdata.ReadLifecycleJSONL(f, defaults)
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		for i := range rows {
			rel, _ := filepath.Rel(*runDir, file)
			rows[i].SourceFile = rel
		}
		events = append(events, rows...)
	}
	summary := perfdata.Summarize(events)
	reportsDir := filepath.Join(*runDir, "reports")
	derivedDir := filepath.Join(*runDir, "derived")
	if err := os.MkdirAll(filepath.Join(reportsDir, "data"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(derivedDir, 0o755); err != nil {
		return err
	}
	if err := writeAllEvents(filepath.Join(derivedDir, "all-lifecycle-events.jsonl"), events); err != nil {
		return err
	}
	if err := writeRoundCSV(filepath.Join(derivedDir, "round-summary.csv"), summary.Rounds); err != nil {
		return err
	}
	if err := writeLatencyCSV(filepath.Join(derivedDir, "latency-percentiles.csv"), summary.LatencyPercentiles); err != nil {
		return err
	}
	if err := writeErrorCSV(filepath.Join(derivedDir, "error-summary.csv"), summary.Errors); err != nil {
		return err
	}
	summaryJSON, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(reportsDir, "data", "summary.json"), summaryJSON, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(reportsDir, "data", "charts.json"), summaryJSON, 0o644); err != nil {
		return err
	}
	id := *runID
	if id == "" {
		id = filepath.Base(*runDir)
	}
	html := perfdata.RenderHTMLReport(summary, perfdata.ReportMetadata{
		Title:       "Substrate + gVisor ACK Performance Report",
		RunID:       id,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err := os.WriteFile(filepath.Join(reportsDir, "index.html"), []byte(html), 0o644); err != nil {
		return err
	}
	if err := writeManifest(*runDir); err != nil {
		return err
	}
	fmt.Printf("report generated: %s\n", filepath.Join(reportsDir, "index.html"))
	return nil
}

func runLifecycle(args []string) error {
	fs := flag.NewFlagSet("run-lifecycle", flag.ExitOnError)
	var cfg runner.LifecycleConfig
	outPath := fs.String("out", "", "output lifecycle JSONL path")
	fs.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "kubeconfig path")
	fs.StringVar(&cfg.RunID, "run-id", "", "run id")
	fs.StringVar(&cfg.Atespace, "atespace", "", "atespace name")
	fs.IntVar(&cfg.Count, "count", 20, "actor count")
	fs.IntVar(&cfg.Concurrency, "concurrency", 1, "concurrency")
	fs.IntVar(&cfg.NodeScale, "node-scale", 0, "node scale label")
	fs.StringVar(&cfg.Round, "round", "", "round label")
	fs.StringVar(&cfg.Stage, "stage", "ateapi_persistent_client", "stage label")
	fs.StringVar(&cfg.Workload, "workload", "counter-smoke", "workload label")
	fs.StringVar(&cfg.ActorPrefix, "actor-prefix", "", "actor name prefix")
	fs.StringVar(&cfg.ActorTemplateNamespace, "actor-template-namespace", "ate-demo-counter", "actor template namespace")
	fs.StringVar(&cfg.ActorTemplateName, "actor-template-name", "counter", "actor template name")
	fs.DurationVar(&cfg.PostWarmResumeSleep, "post-warm-resume-sleep", 0, "optional delay after warm resume before second suspend")
	fs.BoolVar(&cfg.AsyncSuspend, "async-suspend", false, "record async suspend ack and checkpoint-ready wait as separate lifecycle events")
	fs.DurationVar(&cfg.CheckpointReadyTimeout, "checkpoint-ready-timeout", 10*time.Minute, "maximum wait for async suspend to reach SUSPENDED")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outPath == "" {
		return fmt.Errorf("--out is required")
	}
	if target, ok := parseConfigMapOutput(*outPath); ok {
		var buf bytes.Buffer
		err := runner.RunLifecycle(context.Background(), cfg, &buf)
		if writeErr := writeConfigMapChunks(context.Background(), cfg.Kubeconfig, target, buf.Bytes()); writeErr != nil {
			return writeErr
		}
		return err
	}
	out, err := createOutput(*outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	return runner.RunLifecycle(context.Background(), cfg, out)
}

func runCrossNodeRecover(args []string) error {
	fs := flag.NewFlagSet("run-cross-node-recover", flag.ExitOnError)
	var cfg runner.CrossNodeRecoverConfig
	outPath := fs.String("out", "", "output lifecycle JSONL path")
	fs.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "kubeconfig path")
	fs.StringVar(&cfg.RunID, "run-id", "", "run id")
	fs.StringVar(&cfg.Atespace, "atespace", "", "atespace name")
	fs.IntVar(&cfg.Count, "count", 20, "actor count")
	fs.IntVar(&cfg.Concurrency, "concurrency", 1, "concurrency; cross-node recover requires 1")
	fs.IntVar(&cfg.PreBlockers, "preblockers", 0, "number of workers to occupy before each measured actor to steer source placement")
	fs.BoolVar(&cfg.HTTPProbe, "http-probe", false, "probe actor HTTP endpoint through atenet-router before suspend and after cross-node recover")
	fs.StringVar(&cfg.ProbePath, "probe-path", "/", "HTTP path to probe when --http-probe is set")
	fs.DurationVar(&cfg.PostRecoverProbeTimeout, "post-recover-probe-timeout", 0, "maximum time to wait for target HTTP success after cross-node recover; 0 means single probe")
	fs.StringVar(&cfg.DrainBlockerActorTemplateNamespace, "drain-blocker-actor-template-namespace", "", "optional actor template namespace for source-drain blocker actors; defaults to measured actor template namespace")
	fs.StringVar(&cfg.DrainBlockerActorTemplateName, "drain-blocker-actor-template-name", "", "optional actor template name for source-drain blocker actors; defaults to measured actor template name")
	fs.BoolVar(&cfg.CrossNodeResumeTargeting, "cross-node-resume-targeting", false, "force cross-node restore by asking ResumeActor to avoid the source node instead of creating blocker actors")
	fs.IntVar(&cfg.NodeScale, "node-scale", 0, "node scale label")
	fs.StringVar(&cfg.Round, "round", "cross-node-recover-con1", "round label")
	fs.StringVar(&cfg.Stage, "stage", "ateapi_cross_node_recover", "stage label")
	fs.StringVar(&cfg.Workload, "workload", "counter-smoke", "workload label")
	fs.StringVar(&cfg.ActorPrefix, "actor-prefix", "", "actor name prefix")
	fs.StringVar(&cfg.ActorTemplateNamespace, "actor-template-namespace", "ate-demo-counter", "actor template namespace")
	fs.StringVar(&cfg.ActorTemplateName, "actor-template-name", "counter", "actor template name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outPath == "" {
		return fmt.Errorf("--out is required")
	}
	if target, ok := parseConfigMapOutput(*outPath); ok {
		var buf bytes.Buffer
		err := runner.RunCrossNodeRecover(context.Background(), cfg, &buf)
		if writeErr := writeConfigMapChunks(context.Background(), cfg.Kubeconfig, target, buf.Bytes()); writeErr != nil {
			return writeErr
		}
		return err
	}
	out, err := createOutput(*outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	return runner.RunCrossNodeRecover(context.Background(), cfg, out)
}

func runHotMigration(args []string) error {
	fs := flag.NewFlagSet("run-hot-migration", flag.ExitOnError)
	var cfg runner.HotMigrationConfig
	outPath := fs.String("out", "", "output lifecycle JSONL path")
	extraProbePaths := fs.String("extra-probe-paths", "", "comma-separated extra continuous GET probes, optionally label=/path")
	preMigrationPostPaths := fs.String("pre-migration-post-paths", "", "comma-separated POST hooks to run after state mutations and before migration, optionally label=/path")
	fs.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "kubeconfig path")
	fs.StringVar(&cfg.RunID, "run-id", "", "run id")
	fs.StringVar(&cfg.Atespace, "atespace", "", "atespace name")
	fs.IntVar(&cfg.Count, "count", 1, "actor count")
	fs.IntVar(&cfg.Concurrency, "concurrency", 1, "concurrency; hot migration requires 1")
	fs.StringVar(&cfg.ProbePath, "probe-path", "/substrate/migration-state", "HTTP path to probe continuously")
	fs.StringVar(&cfg.SSEPath, "sse-path", "", "optional SSE path to keep open during migration, e.g. /stream")
	fs.StringVar(&cfg.TerminalWebSocketPath, "terminal-websocket-path", "", "optional WebSocket terminal path to keep open during migration, e.g. /terminal/ws")
	fs.StringVar(&cfg.MutationPath, "mutation-path", "/increment", "HTTP path to POST before migration to create runtime state")
	fs.IntVar(&cfg.StateMutations, "state-mutations", 1, "number of pre-migration mutation POSTs")
	fs.DurationVar(&cfg.MutationInterval, "continuous-mutation-interval", 0, "optional interval for continuous POST mutations during hot migration; 0 disables continuous mutations")
	fs.Int64Var(&cfg.MutationBodyBytes, "mutation-body-bytes", 0, "optional request body size for mutation POSTs, used for upload scenarios")
	fs.DurationVar(&cfg.ProbeInterval, "probe-interval", 100*time.Millisecond, "continuous HTTP probe interval")
	fs.Int64Var(&cfg.ProbeReadLimitBytes, "probe-read-limit-bytes", 64*1024, "maximum response body bytes to read for each HTTP probe")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", 3*time.Second, "CommitActorMigration drain timeout")
	fs.DurationVar(&cfg.PostCommitProbeTimeout, "post-commit-probe-timeout", 30*time.Second, "maximum time to wait for target HTTP success after commit")
	fs.DurationVar(&cfg.CleanupIsolationDuration, "cleanup-isolation-duration", 0, "optional post-switch observation duration to keep probes running after target success/source release; final actor suspend/delete runs after summary")
	fs.DurationVar(&cfg.MigrationRPCTimeout, "migration-rpc-timeout", 180*time.Second, "maximum time to wait for prepare or commit migration RPC")
	fs.Int64Var(&cfg.LiveMigrationDowntimeMS, "live-migration-downtime-ms", 0, "expected Cloud Hypervisor live migration downtime_ms; recorded in raw evidence only")
	fs.IntVar(&cfg.LiveMigrationConnections, "live-migration-connections", 0, "expected Cloud Hypervisor live migration connection count; recorded in raw evidence only")
	fs.StringVar(&cfg.LiveMigrationMemoryMode, "live-migration-memory-mode", "", "expected Cloud Hypervisor live migration memory mode; recorded in raw evidence only")
	fs.BoolVar(&cfg.RequireRouteSwitchedProbe, "require-route-switched-probe", false, "require at least one successful route PHASE_SWITCHED probe sample during commit; used for source-release isolation evidence")
	fs.BoolVar(&cfg.SkipFinalActorCleanup, "skip-final-actor-cleanup", false, "skip post-summary suspend/delete; caller must clean test workers")
	fs.DurationVar(&cfg.ActorTemplateTimeout, "actor-template-timeout", 30*time.Minute, "maximum time to wait for ActorTemplate Ready with a golden snapshot")
	fs.BoolVar(&cfg.RequireCrossNode, "require-cross-node", true, "require target worker on a different node")
	fs.BoolVar(&cfg.RequireQuiesce, "require-quiesce", false, "request control plane quiesce during commit when supported")
	fs.BoolVar(&cfg.BootSource, "boot-source", false, "boot the source actor from scratch instead of waiting for a golden snapshot before hot migration")
	targetWorkerLabels := fs.String("target-worker-labels", "", "comma-separated target worker selector labels, e.g. disk=raid,zone=a")
	fs.IntVar(&cfg.NodeScale, "node-scale", 0, "node scale label")
	fs.StringVar(&cfg.Round, "round", "hot-migration-con1", "round label")
	fs.StringVar(&cfg.Stage, "stage", "ateapi_hot_migration", "stage label")
	fs.StringVar(&cfg.Workload, "workload", "memory-16g", "workload label")
	fs.StringVar(&cfg.ActorPrefix, "actor-prefix", "", "actor name prefix")
	fs.StringVar(&cfg.ActorTemplateNamespace, "actor-template-namespace", "ate-demo-memory-gradient", "actor template namespace")
	fs.StringVar(&cfg.ActorTemplateName, "actor-template-name", "memory-16g", "actor template name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outPath == "" {
		return fmt.Errorf("--out is required")
	}
	if *extraProbePaths != "" {
		cfg.ExtraProbePaths = splitNonEmptyCSV(*extraProbePaths)
	}
	if *preMigrationPostPaths != "" {
		cfg.PreMigrationPostPaths = splitNonEmptyCSV(*preMigrationPostPaths)
	}
	labels, err := parseLabelMap(*targetWorkerLabels)
	if err != nil {
		return err
	}
	cfg.TargetWorkerLabels = labels
	if target, ok := parseConfigMapOutput(*outPath); ok {
		var buf bytes.Buffer
		err := runner.RunHotMigration(context.Background(), cfg, &buf)
		if writeErr := writeConfigMapChunks(context.Background(), cfg.Kubeconfig, target, buf.Bytes()); writeErr != nil {
			return writeErr
		}
		return err
	}
	out, err := createOutput(*outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	return runner.RunHotMigration(context.Background(), cfg, out)
}

func runCleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup-atespace", flag.ExitOnError)
	kubeconfig := fs.String("kubeconfig", "", "kubeconfig path")
	atespace := fs.String("atespace", "", "atespace name")
	concurrency := fs.Int("concurrency", 8, "cleanup concurrency")
	outPath := fs.String("out", "", "output cleanup JSONL path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *outPath == "" {
		return fmt.Errorf("--out is required")
	}
	out, err := createOutput(*outPath)
	if err != nil {
		return err
	}
	defer out.Close()
	return runner.CleanupAtespace(context.Background(), *kubeconfig, *atespace, *concurrency, out)
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}

func createOutput(path string) (io.WriteCloser, error) {
	if path == "-" {
		return nopWriteCloser{Writer: os.Stdout}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

type configMapOutput struct {
	namespace string
	prefix    string
}

func parseConfigMapOutput(path string) (configMapOutput, bool) {
	const scheme = "k8s-configmaps://"
	if !strings.HasPrefix(path, scheme) {
		return configMapOutput{}, false
	}
	rest := strings.TrimPrefix(path, scheme)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return configMapOutput{}, false
	}
	return configMapOutput{namespace: parts[0], prefix: parts[1]}, true
}

func parseLabelMap(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("invalid label %q, want key=value", part)
		}
		out[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return out, nil
}

func splitNonEmptyCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func writeConfigMapChunks(ctx context.Context, kubeconfig string, target configMapOutput, data []byte) error {
	cfg, err := ateclient.LoadConfig(kubeconfig, "")
	if err != nil {
		return fmt.Errorf("load kubeconfig for configmap output: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("create k8s client for configmap output: %w", err)
	}
	const chunkSize = 12 * 1024
	chunks := 0
	for offset := 0; offset < len(data) || (len(data) == 0 && offset == 0); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]
		name := fmt.Sprintf("%s-%04d", target.prefix, chunks)
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: target.namespace,
				Labels: map[string]string{
					"perfkit.ate.dev/output-prefix": target.prefix,
					"perfkit.ate.dev/chunk-index":   fmt.Sprintf("%04d", chunks),
				},
			},
			Data: map[string]string{
				"events.b64": base64.StdEncoding.EncodeToString(chunk),
			},
		}
		if _, err := client.CoreV1().ConfigMaps(target.namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create configmap chunk %s: %w", name, err)
			}
			if _, err := client.CoreV1().ConfigMaps(target.namespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("update configmap chunk %s: %w", name, err)
			}
		}
		chunks++
		if end == len(data) {
			break
		}
	}
	index := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target.prefix + "-index",
			Namespace: target.namespace,
			Labels: map[string]string{
				"perfkit.ate.dev/output-prefix": target.prefix,
			},
		},
		Data: map[string]string{
			"chunks": fmt.Sprintf("%d", chunks),
		},
	}
	if _, err := client.CoreV1().ConfigMaps(target.namespace).Create(ctx, index, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create configmap index: %w", err)
		}
		if _, err := client.CoreV1().ConfigMaps(target.namespace).Update(ctx, index, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update configmap index: %w", err)
		}
	}
	fmt.Fprintf(os.Stderr, "wrote %d configmap chunks: %s/%s\n", chunks, target.namespace, target.prefix)
	return nil
}

func lifecycleFiles(runDir, explicit string) ([]string, error) {
	if explicit != "" {
		var files []string
		for _, file := range strings.Split(explicit, ",") {
			file = strings.TrimSpace(file)
			if file != "" {
				files = append(files, file)
			}
		}
		return files, nil
	}
	files, err := filepath.Glob(filepath.Join(runDir, "raw", "substrate-api", "lifecycle-events*.jsonl"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no lifecycle event files found")
	}
	sortLifecycleFiles(files)
	return files, nil
}

func defaultsForFile(file string) perfdata.Defaults {
	base := filepath.Base(file)
	defaults := perfdata.Defaults{Round: strings.TrimSuffix(strings.TrimPrefix(base, "lifecycle-events-"), ".jsonl"), NodeScale: 10, Concurrency: 1}
	if base == "lifecycle-events.jsonl" {
		defaults.Round = "r0-cli-smoke"
	}
	switch {
	case strings.Contains(base, "r2con10"):
		defaults.Round = "r2-con10"
		defaults.Concurrency = 10
	case strings.Contains(base, "r2con3"):
		defaults.Round = "r2-con3"
		defaults.Concurrency = 3
	case strings.Contains(base, "r2seq100"):
		defaults.Round = "r2-seq100"
	case strings.Contains(base, "r1lite"):
		defaults.Round = "r1-lite"
	}
	return defaults
}

func sortLifecycleFiles(files []string) {
	order := map[string]int{
		"lifecycle-events.jsonl":          0,
		"lifecycle-events-r1lite.jsonl":   1,
		"lifecycle-events-r2seq100.jsonl": 2,
		"lifecycle-events-r2con3.jsonl":   3,
		"lifecycle-events-r2con10.jsonl":  4,
	}
	sort.Slice(files, func(i, j int) bool {
		oi, okI := order[filepath.Base(files[i])]
		oj, okJ := order[filepath.Base(files[j])]
		if okI && okJ {
			return oi < oj
		}
		if okI != okJ {
			return okI
		}
		return files[i] < files[j]
	})
}

func writeAllEvents(path string, events []perfdata.LifecycleEvent) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
	return nil
}

func writeRoundCSV(path string, rows []perfdata.RoundSummary) error {
	return writeCSV(path, []string{"round", "node_scale", "concurrency", "actors", "operations", "success", "errors", "error_rate", "duration_s", "ops_per_s"}, func(w *csv.Writer) error {
		for _, r := range rows {
			if err := w.Write([]string{r.Round, itoa(r.NodeScale), itoa(r.Concurrency), itoa(r.Actors), itoa(r.Operations), itoa(r.Success), itoa(r.Errors), ftoa(r.ErrorRate), ftoa(r.DurationS), ftoa(r.OpsPerS)}); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeLatencyCSV(path string, rows []perfdata.LatencySummary) error {
	return writeCSV(path, []string{"round", "operation", "node_scale", "concurrency", "samples", "success", "errors", "error_rate", "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms"}, func(w *csv.Writer) error {
		for _, r := range rows {
			if err := w.Write([]string{r.Round, r.Operation, itoa(r.NodeScale), itoa(r.Concurrency), itoa(r.Samples), itoa(r.Success), itoa(r.Errors), ftoa(r.ErrorRate), ftoa(r.AvgMS), ftoa(r.P50MS), ftoa(r.P90MS), ftoa(r.P95MS), ftoa(r.P99MS), ftoa(r.MaxMS)}); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeErrorCSV(path string, rows []perfdata.ErrorSummary) error {
	return writeCSV(path, []string{"round", "operation", "error_code", "error_reason", "count"}, func(w *csv.Writer) error {
		for _, r := range rows {
			if err := w.Write([]string{r.Round, r.Operation, r.ErrorCode, r.ErrorReason, itoa(r.Count)}); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeCSV(path string, header []string, writeRows func(*csv.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write(header); err != nil {
		return err
	}
	if err := writeRows(w); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func writeManifest(runDir string) error {
	type entry struct {
		Path   string `json:"path"`
		Bytes  int64  `json:"bytes"`
		SHA256 string `json:"sha256"`
	}
	var entries []entry
	err := filepath.WalkDir(runDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(runDir, path)
		entries = append(entries, entry{Path: rel, Bytes: info.Size(), SHA256: hex.EncodeToString(h.Sum(nil))})
		return nil
	})
	if err != nil {
		return err
	}
	out, err := json.MarshalIndent(map[string]any{
		"generated_at": time.Now().UTC().Format(time.RFC3339),
		"files":        entries,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, "manifest.json"), out, 0o644)
}

func itoa(v int) string {
	return strconv.Itoa(v)
}

func ftoa(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
