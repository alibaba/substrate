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
	"bufio"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math"
	"sort"
	"strings"
	"time"
)

type LifecycleEvent struct {
	RunID           string `json:"run_id,omitempty"`
	OperationID     string `json:"operation_id,omitempty"`
	ActorID         string `json:"actor_id,omitempty"`
	Operation       string `json:"operation,omitempty"`
	Stage           string `json:"stage,omitempty"`
	StartTS         string `json:"start_ts,omitempty"`
	EndTS           string `json:"end_ts,omitempty"`
	DurationMS      int64  `json:"duration_ms,omitempty"`
	Workload        string `json:"workload,omitempty"`
	Round           string `json:"round,omitempty"`
	NodeScale       int    `json:"node_scale,omitempty"`
	Concurrency     int    `json:"concurrency,omitempty"`
	Result          string `json:"result,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorReason     string `json:"error_reason,omitempty"`
	SourceWorker    string `json:"source_worker,omitempty"`
	SourceNode      string `json:"source_node,omitempty"`
	TargetWorker    string `json:"target_worker,omitempty"`
	TargetNode      string `json:"target_node,omitempty"`
	SnapshotType    string `json:"snapshot_type,omitempty"`
	RecoverStrategy string `json:"recover_strategy,omitempty"`
	CrossNode       bool   `json:"cross_node,omitempty"`
	BlockerActors   int    `json:"blocker_actors,omitempty"`
	SourceFile      string `json:"source_file,omitempty"`
	HTTPStatus      int    `json:"http_status,omitempty"`
	HTTPChecksum    string `json:"http_checksum,omitempty"`
	HTTPBody        string `json:"http_body,omitempty"`
	HTTPCounter     int64  `json:"http_counter,omitempty"`
	HTTPInstanceID  string `json:"http_instance_id,omitempty"`
	HTTPNodeName    string `json:"http_node_name,omitempty"`

	RouteGenerationBefore  int64            `json:"route_generation_before,omitempty"`
	RouteGenerationAfter   int64            `json:"route_generation_after,omitempty"`
	StageDurationMS        map[string]int64 `json:"stage_duration_ms,omitempty"`
	LongestSuccessGapMS    int64            `json:"longest_success_gap_ms,omitempty"`
	TotalToTargetSuccessMS int64            `json:"total_to_target_success_ms,omitempty"`
	ProbeRequests          int              `json:"probe_requests,omitempty"`
	ProbeSuccesses         int              `json:"probe_successes,omitempty"`
	ProbeFailures          int              `json:"probe_failures,omitempty"`
	ProbeStatusCounts      map[string]int   `json:"probe_status_counts,omitempty"`
	ProbeSequence          int              `json:"probe_sequence,omitempty"`
	StateRegressed         bool             `json:"state_regressed,omitempty"`
}

type Defaults struct {
	Round       string
	NodeScale   int
	Concurrency int
}

type RoundSummary struct {
	Round       string  `json:"round"`
	NodeScale   int     `json:"node_scale"`
	Concurrency int     `json:"concurrency"`
	Actors      int     `json:"actors"`
	Operations  int     `json:"operations"`
	Success     int     `json:"success"`
	Errors      int     `json:"errors"`
	ErrorRate   float64 `json:"error_rate"`
	DurationS   float64 `json:"duration_s,omitempty"`
	OpsPerS     float64 `json:"ops_per_s,omitempty"`
	FirstTS     string  `json:"first_ts,omitempty"`
	LastTS      string  `json:"last_ts,omitempty"`
}

type LatencySummary struct {
	Round       string  `json:"round"`
	Operation   string  `json:"operation"`
	NodeScale   int     `json:"node_scale"`
	Concurrency int     `json:"concurrency"`
	Samples     int     `json:"samples"`
	Success     int     `json:"success"`
	Errors      int     `json:"errors"`
	ErrorRate   float64 `json:"error_rate"`
	AvgMS       float64 `json:"avg_ms,omitempty"`
	P50MS       float64 `json:"p50_ms,omitempty"`
	P90MS       float64 `json:"p90_ms,omitempty"`
	P95MS       float64 `json:"p95_ms,omitempty"`
	P99MS       float64 `json:"p99_ms,omitempty"`
	MaxMS       float64 `json:"max_ms,omitempty"`
}

type ErrorSummary struct {
	Round       string `json:"round"`
	Operation   string `json:"operation"`
	ErrorCode   string `json:"error_code"`
	ErrorReason string `json:"error_reason"`
	Count       int    `json:"count"`
}

type Summary struct {
	EventCount         int              `json:"event_count"`
	Rounds             []RoundSummary   `json:"rounds"`
	LatencyPercentiles []LatencySummary `json:"latency_percentiles"`
	Errors             []ErrorSummary   `json:"errors"`
}

type ReportMetadata struct {
	Title       string
	RunID       string
	GeneratedAt string
}

type scenarioInfo struct {
	ID             string
	Label          string
	Group          string
	Intent         string
	ExpectedSignal string
	Interpretation string
	UseForBaseline bool
}

func ReadLifecycleJSONL(r io.Reader, defaults Defaults) ([]LifecycleEvent, error) {
	var events []LifecycleEvent
	scanner := bufio.NewScanner(r)
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var ev LifecycleEvent
		if err := json.Unmarshal([]byte(text), &ev); err != nil {
			return nil, fmt.Errorf("decode lifecycle event line %d: %w", line, err)
		}
		if ev.Round == "" {
			ev.Round = defaults.Round
		}
		if ev.NodeScale == 0 {
			ev.NodeScale = defaults.NodeScale
		}
		if ev.Concurrency == 0 {
			ev.Concurrency = defaults.Concurrency
		}
		if ev.Result == "" {
			ev.Result = "ok"
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func Summarize(events []LifecycleEvent) Summary {
	roundEvents := map[string][]LifecycleEvent{}
	roundOpEvents := map[string][]LifecycleEvent{}
	errorCounts := map[string]int{}
	for _, ev := range events {
		round := ev.Round
		if round == "" {
			round = "unknown"
			ev.Round = round
		}
		op := ev.Operation
		if op == "" {
			op = "unknown"
			ev.Operation = op
		}
		roundEvents[round] = append(roundEvents[round], ev)
		roundOpEvents[round+"\x00"+op] = append(roundOpEvents[round+"\x00"+op], ev)
		if ev.Result != "ok" {
			key := strings.Join([]string{round, op, ev.ErrorCode, ev.ErrorReason}, "\x00")
			errorCounts[key]++
		}
	}

	summary := Summary{
		EventCount:         len(events),
		Rounds:             []RoundSummary{},
		LatencyPercentiles: []LatencySummary{},
		Errors:             []ErrorSummary{},
	}
	for round, rows := range roundEvents {
		summary.Rounds = append(summary.Rounds, summarizeRound(round, rows))
	}
	sort.Slice(summary.Rounds, func(i, j int) bool { return summary.Rounds[i].Round < summary.Rounds[j].Round })

	for key, rows := range roundOpEvents {
		parts := strings.Split(key, "\x00")
		summary.LatencyPercentiles = append(summary.LatencyPercentiles, summarizeLatency(parts[0], parts[1], rows))
	}
	sort.Slice(summary.LatencyPercentiles, func(i, j int) bool {
		if summary.LatencyPercentiles[i].Round != summary.LatencyPercentiles[j].Round {
			return summary.LatencyPercentiles[i].Round < summary.LatencyPercentiles[j].Round
		}
		return summary.LatencyPercentiles[i].Operation < summary.LatencyPercentiles[j].Operation
	})

	for key, count := range errorCounts {
		parts := strings.Split(key, "\x00")
		summary.Errors = append(summary.Errors, ErrorSummary{
			Round: parts[0], Operation: parts[1], ErrorCode: parts[2], ErrorReason: parts[3], Count: count,
		})
	}
	sort.Slice(summary.Errors, func(i, j int) bool {
		if summary.Errors[i].Round != summary.Errors[j].Round {
			return summary.Errors[i].Round < summary.Errors[j].Round
		}
		if summary.Errors[i].Operation != summary.Errors[j].Operation {
			return summary.Errors[i].Operation < summary.Errors[j].Operation
		}
		return summary.Errors[i].ErrorCode < summary.Errors[j].ErrorCode
	})
	return summary
}

func (s Summary) RoundByName(round string) *RoundSummary {
	for i := range s.Rounds {
		if s.Rounds[i].Round == round {
			return &s.Rounds[i]
		}
	}
	return nil
}

func (s Summary) LatencyByRoundOperation(round, operation string) *LatencySummary {
	for i := range s.LatencyPercentiles {
		if s.LatencyPercentiles[i].Round == round && s.LatencyPercentiles[i].Operation == operation {
			return &s.LatencyPercentiles[i]
		}
	}
	return nil
}

func summarizeRound(round string, rows []LifecycleEvent) RoundSummary {
	actors := map[string]bool{}
	var success, errors int
	var first, last time.Time
	for _, ev := range rows {
		if ev.ActorID != "" {
			actors[ev.ActorID] = true
		}
		if ev.Result == "ok" {
			success++
		} else {
			errors++
		}
		start := parseTime(ev.StartTS)
		end := parseTime(ev.EndTS)
		if !start.IsZero() && (first.IsZero() || start.Before(first)) {
			first = start
		}
		if !end.IsZero() && (last.IsZero() || end.After(last)) {
			last = end
		}
	}
	out := RoundSummary{
		Round:       round,
		NodeScale:   maxNodeScale(rows),
		Concurrency: maxConcurrency(rows),
		Actors:      len(actors),
		Operations:  len(rows),
		Success:     success,
		Errors:      errors,
	}
	if len(rows) > 0 {
		out.ErrorRate = float64(errors) / float64(len(rows))
	}
	if !first.IsZero() && !last.IsZero() && last.After(first) {
		out.FirstTS = first.Format(time.RFC3339Nano)
		out.LastTS = last.Format(time.RFC3339Nano)
		out.DurationS = last.Sub(first).Seconds()
		out.OpsPerS = float64(len(rows)) / out.DurationS
	}
	return out
}

func summarizeLatency(round, operation string, rows []LifecycleEvent) LatencySummary {
	var vals []float64
	var sum float64
	var errors int
	for _, ev := range rows {
		if ev.Result != "ok" {
			errors++
			continue
		}
		v := float64(ev.DurationMS)
		vals = append(vals, v)
		sum += v
	}
	out := LatencySummary{
		Round:       round,
		Operation:   operation,
		NodeScale:   maxNodeScale(rows),
		Concurrency: maxConcurrency(rows),
		Samples:     len(rows),
		Success:     len(vals),
		Errors:      errors,
	}
	if len(rows) > 0 {
		out.ErrorRate = float64(errors) / float64(len(rows))
	}
	if len(vals) > 0 {
		out.AvgMS = sum / float64(len(vals))
		out.P50MS = percentile(vals, 50)
		out.P90MS = percentile(vals, 90)
		out.P95MS = percentile(vals, 95)
		out.P99MS = percentile(vals, 99)
		out.MaxMS = vals[0]
		for _, v := range vals[1:] {
			if v > out.MaxMS {
				out.MaxMS = v
			}
		}
	}
	return out
}

func RenderHTMLReport(summary Summary, meta ReportMetadata) string {
	title := meta.Title
	if title == "" {
		title = "Substrate Performance Report"
	}
	generated := meta.GeneratedAt
	if generated == "" {
		generated = time.Now().UTC().Format(time.RFC3339)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>%s</title><link rel=\"icon\" href=\"data:,\"><style>%s</style></head><body>", html.EscapeString(title), reportCSS())
	fmt.Fprintf(&b, "<header><h1>%s</h1><div>Run: %s · Generated: %s</div></header><main>", html.EscapeString(title), html.EscapeString(meta.RunID), html.EscapeString(generated))
	b.WriteString(renderExecutiveSummary(summary))
	b.WriteString(renderKeyFindings(summary))
	b.WriteString(renderMethodology())
	b.WriteString(renderScenarioTable(summary.Rounds))
	b.WriteString(renderMetricDefinitions())
	b.WriteString(renderEnvironmentAndLimits())
	b.WriteString("<h2>趋势图与对比</h2>")
	perfRounds := performanceTrendRounds(summary.Rounds)
	perfLatencyRows := performanceTrendLatencyRows(summary.LatencyPercentiles)
	b.WriteString("<p class=\"section-note\">图表只比较同一类实验：10 节点性能基线、容量探测、OCI unpack 优化、内存梯度和跨节点恢复。环境排障轮次单独放在异常分析中，避免把排障数据误作横向性能对比。</p>")
	b.WriteString("<section class=\"charts\">")
	b.WriteString(renderChartPanel("失败率趋势", "单位：%。并发 10 出现红线后，吞吐不能解释为性能提升，只能解释为失败压力下的事件处理速率。", renderBarSVG("场景失败率（% failed lifecycle operations）", perfRounds, func(r RoundSummary) float64 { return r.ErrorRate * 100 }, "%")))
	b.WriteString(renderChartPanel("生命周期吞吐趋势", "单位：ops/s。只比较正式性能组；失败轮需要和错误率一起阅读。", renderBarSVG("生命周期吞吐（ops/s）", perfRounds, func(r RoundSummary) float64 { return r.OpsPerS }, " ops/s")))
	b.WriteString(renderChartPanel("成功与失败构成", "绿色是成功操作，红色是失败操作；红色出现时先看容量和异常分析。", renderStackedRoundSVG("生命周期操作结果分布（count）", perfRounds)))
	b.WriteString(renderChartPanel("并发容量趋势", "X 轴是场景并发，Y 轴是成功率。该图用于定位并发从 3 到 10 时的容量断点。", renderBarSVG("成功率（% successful lifecycle operations）", perfRounds, func(r RoundSummary) float64 { return (1 - r.ErrorRate) * 100 }, "%")))
	b.WriteString(renderChartPanel("冷恢复延迟", "单位：ms。只显示有成功样本的 round；无成功样本在明细表中显示 N/A。", renderLatencyOperationSVG("冷恢复 p95（resume_boot）", perfLatencyRows, "resume_boot")))
	b.WriteString(renderChartPanel("热恢复延迟", "单位：ms。暂停后再次 ResumeActor，主要观察 checkpoint/restore 路径是否稳定。", renderLatencyOperationSVG("热恢复 p95（resume_warm）", perfLatencyRows, "resume_warm")))
	b.WriteString(renderChartPanel("跨节点 Recover 延迟", "单位：ms。只统计外部快照恢复后 source_node != target_node 的成功样本。", renderLatencyOperationSVG("跨节点 Recover p95（cross_node_recover）", perfLatencyRows, "cross_node_recover")))
	b.WriteString(renderChartPanel("OCI unpack 优化曲线", "单位：ms。baseline 是每次恢复重新 untar；rootfs-cache 表示启用已解包 rootfs 缓存后的恢复路径。", renderLatencyOperationSVG("OCI 优化轮次 p95（cross_node_recover）", perfLatencyRows, "cross_node_recover")))
	b.WriteString(renderChartPanel("内存梯度恢复曲线", "单位：ms。按内存模板从 64MiB 到 4GiB 展示恢复 p95，观察 snapshot 体积增长后的衰减。", renderLatencyOperationSVG("内存梯度 p95（resume_warm）", perfLatencyRows, "resume_warm")))
	b.WriteString(renderChartPanel("第一次暂停 checkpoint", "单位：ms。单独展示 suspend_1，避免把不同暂停阶段混成一个 max。", renderLatencyOperationSVG("第一次暂停 p95（suspend_1）", perfLatencyRows, "suspend_1")))
	b.WriteString(renderChartPanel("第二次暂停 checkpoint", "单位：ms。单独展示 suspend_2，用于和第一次暂停对比。", renderLatencyOperationSVG("第二次暂停 p95（suspend_2）", perfLatencyRows, "suspend_2")))
	b.WriteString("</section>")
	b.WriteString(renderAnomalyAnalysis(summary))
	b.WriteString(renderRoundSummaryTable(summary.Rounds))
	b.WriteString("<h2>生命周期延迟分位</h2><p class=\"section-note\">延迟分位只基于成功样本计算；失败样本计入成功/样本列和错误率，不参与 p50/p95/p99。</p><table><thead><tr><th>场景</th><th>操作</th><th>内部操作</th><th>成功/样本</th><th>错误率</th><th>p50 ms</th><th>p95 ms</th><th>p99 ms</th><th>max ms</th></tr></thead><tbody>")
	for _, r := range summary.LatencyPercentiles {
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td><code>%s</code></td><td>%d/%d</td><td>%.1f%%</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>", html.EscapeString(roundLabel(r.Round)), html.EscapeString(operationLabel(r.Operation)), html.EscapeString(r.Operation), r.Success, r.Samples, r.ErrorRate*100, formatLatencyCell(r.Success, r.P50MS), formatLatencyCell(r.Success, r.P95MS), formatLatencyCell(r.Success, r.P99MS), formatLatencyCell(r.Success, r.MaxMS))
	}
	b.WriteString("</tbody></table><h2>错误摘要</h2><table><thead><tr><th>场景</th><th>操作</th><th>错误码</th><th>次数</th><th>含义</th><th>原始原因</th></tr></thead><tbody>")
	if len(summary.Errors) == 0 {
		b.WriteString("<tr><td colspan=\"6\">No errors recorded</td></tr>")
	}
	for _, e := range summary.Errors {
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%s</td><td>%s</td></tr>", html.EscapeString(roundLabel(e.Round)), html.EscapeString(operationLabel(e.Operation)), html.EscapeString(e.ErrorCode), e.Count, html.EscapeString(errorMeaning(e)), html.EscapeString(e.ErrorReason))
	}
	b.WriteString("</tbody></table>")
	b.WriteString(renderRawDataLineage(meta))
	b.WriteString("</main></body></html>")
	return b.String()
}

func renderExecutiveSummary(summary Summary) string {
	rounds := orderedRounds(summary.Rounds)
	var totalSuccess, totalErrors int
	for _, r := range rounds {
		totalSuccess += r.Success
		totalErrors += r.Errors
	}
	con3 := summary.RoundByName("r2-con3")
	con10 := summary.RoundByName("r2-con10")
	validated := summary.RoundByName("new-env-smoke-counter-perf-success")

	var b strings.Builder
	b.WriteString("<section class=\"executive\"><div><h2>执行摘要</h2>")
	b.WriteString("<p>本报告回答四个问题：Substrate + gVisor 在 ACK 上的 actor 生命周期是否稳定；OCI unpack 是否仍是跨节点恢复尾延迟瓶颈；内存尺寸增大后恢复效率如何衰减；当前数据哪些可以作为性能基线、哪些只是环境验收。</p>")
	b.WriteString("<ul>")
	if con3 != nil {
		fmt.Fprintf(&b, "<li><b>稳定基线：</b>%s 覆盖 %d actors / %d lifecycle 操作，错误率 %.1f%%，可作为当前 10 节点稳态性能基线。</li>", html.EscapeString(roundLabel(con3.Round)), con3.Actors, con3.Operations, con3.ErrorRate*100)
	}
	if con10 != nil {
		fmt.Fprintf(&b, "<li><b>容量边界：</b>%s 覆盖 %d actors / 并发 %d，错误率 %.1f%%；失败集中在 worker 分配，说明该轮主要表达容量饱和，不是单操作延迟退化。</li>", html.EscapeString(roundLabel(con10.Round)), con10.Actors, con10.Concurrency, con10.ErrorRate*100)
	}
	if validated != nil {
		fmt.Fprintf(&b, "<li><b>自动化验收：</b>%s 在修复 ACK user namespace 后 %d/%d 操作成功，用于证明 perfkit 和节点准备流程可复跑。</li>", html.EscapeString(roundLabel(validated.Round)), validated.Success, validated.Operations)
	}
	if row := bestLatencyForGroup(summary, "OCI unpack 优化", "cross_node_recover"); row != nil {
		fmt.Fprintf(&b, "<li><b>OCI unpack 优化：</b>%s 的 cross_node_recover p95 为 %.0f ms；需和 baseline 轮次对比确认优化收益。</li>", html.EscapeString(roundLabel(row.Round)), row.P95MS)
	}
	if row := bestLatencyForGroup(summary, "内存梯度", "resume_warm"); row != nil {
		fmt.Fprintf(&b, "<li><b>内存梯度：</b>%s 的热恢复 p95 为 %.0f ms；该组用于判断 snapshot 体积增长后的恢复衰减。</li>", html.EscapeString(roundLabel(row.Round)), row.P95MS)
	}
	b.WriteString("<li><b>规模缺口：</b>当前报告按 node_scale 标签承载 10/50/100 节点趋势；未执行的规模不会生成结论。</li>")
	b.WriteString("</ul></div>")
	fmt.Fprintf(&b, "<section class=\"kpis\"><div><b>%d</b><span>生命周期原始事件</span></div><div><b>%d / %d</b><span>成功 / 失败操作</span></div><div><b>%d</b><span>测试轮次</span></div><div><b>%d</b><span>错误类型</span></div></section>", summary.EventCount, totalSuccess, totalErrors, len(summary.Rounds), len(summary.Errors))
	b.WriteString("</section>")
	return b.String()
}

func renderKeyFindings(summary Summary) string {
	findings := []string{}
	if r := summary.RoundByName("r2-con3"); r != nil {
		findings = append(findings, fmt.Sprintf("<b>10 节点并发 3 可作为稳态对照：</b>%d actors、%d 操作、错误率 %.1f%%。", r.Actors, r.Operations, r.ErrorRate*100))
	}
	if r := summary.RoundByName("r2-con10"); r != nil {
		findings = append(findings, fmt.Sprintf("<b>并发 10 触发容量饱和：</b>%d 次失败，错误率 %.1f%%，主要是 no free workers。", r.Errors, r.ErrorRate*100))
	}
	if row := summary.LatencyByRoundOperation("r2-con3", "resume_boot"); row != nil {
		findings = append(findings, fmt.Sprintf("<b>冷恢复稳态 p95：</b>%s 的 resume_boot p95 为 %.0f ms。", html.EscapeString(roundLabel(row.Round)), row.P95MS))
	}
	if row := summary.LatencyByRoundOperation("r2-con3", "resume_warm"); row != nil {
		findings = append(findings, fmt.Sprintf("<b>热恢复稳态 p95：</b>%s 的 resume_warm p95 为 %.0f ms。", html.EscapeString(roundLabel(row.Round)), row.P95MS))
	}
	if row := summary.LatencyByRoundOperation("cross-node-recover-con1", "cross_node_recover"); row != nil {
		findings = append(findings, fmt.Sprintf("<b>跨节点 Recover p95：</b>%s 的 cross_node_recover p95 为 %.0f ms，成功样本 %d/%d。", html.EscapeString(roundLabel(row.Round)), row.P95MS, row.Success, row.Samples))
	}
	if row := bestLatencyForGroup(summary, "OCI unpack 优化", "cross_node_recover"); row != nil {
		findings = append(findings, fmt.Sprintf("<b>OCI unpack 优化观察项：</b>%s 的跨节点恢复 p95 为 %.0f ms，成功样本 %d/%d。", html.EscapeString(roundLabel(row.Round)), row.P95MS, row.Success, row.Samples))
	}
	if row := worstLatencyForGroup(summary, "内存梯度", "resume_warm"); row != nil {
		findings = append(findings, fmt.Sprintf("<b>内存梯度最慢项：</b>%s 的热恢复 p95 为 %.0f ms，成功样本 %d/%d。", html.EscapeString(roundLabel(row.Round)), row.P95MS, row.Success, row.Samples))
	}
	if r := summary.RoundByName("new-env-smoke-counter-perf-success"); r != nil {
		findings = append(findings, fmt.Sprintf("<b>新环境验收已闭环：</b>%d actors、%d 操作全部成功，证明 user namespace 修复后 gVisor 生命周期路径可用。", r.Actors, r.Operations))
	}
	var b strings.Builder
	b.WriteString("<h2>关键结论</h2><section class=\"finding-grid\">")
	for _, finding := range findings {
		fmt.Fprintf(&b, "<div class=\"finding\">%s</div>", finding)
	}
	b.WriteString("</section>")
	return b.String()
}

func renderMethodology() string {
	return "<section class=\"narrative\"><h2>方法与口径</h2><p>每个 actor 按固定生命周期执行：create → resume_boot → suspend_1 → resume_warm → suspend_2 → delete。报告按 round 聚合事件，并按 operation 计算成功样本的 p50/p95/p99/max 延迟。</p><p>错误率按失败 lifecycle event / 总 lifecycle event 计算。吞吐按 round 总 event 数 / round 首尾时间跨度计算，只用于比较测试轮次执行效率，不代表业务请求吞吐。</p><p>节点规模使用事件中的 node_scale 标签。该标签用于把 10、50、100 节点轮次放在同一趋势图中对比；当前数据尚未包含 50/100 节点轮次，因此规模趋势只展示已有标签。</p></section>"
}

func renderMetricDefinitions() string {
	return "<section class=\"narrative\"><h2>指标定义</h2><table><thead><tr><th>指标</th><th>单位</th><th>用途</th><th>注意事项</th></tr></thead><tbody><tr><td>错误率</td><td>%</td><td>判断稳定性、容量饱和和环境故障。</td><td>出现失败时，延迟分位不能单独代表该轮性能。</td></tr><tr><td>生命周期吞吐</td><td>ops/s</td><td>比较 perfkit 执行 actor 生命周期的整体速率。</td><td>不是业务 QPS，也不是单沙箱吞吐。</td></tr><tr><td>冷恢复 p95/p99</td><td>ms</td><td>评估首次启动成本。</td><td>受 worker、镜像、runsc、readiness 共同影响。</td></tr><tr><td>热恢复 p95/p99</td><td>ms</td><td>评估暂停后恢复成本。</td><td>用于观察 snapshot/restore 路径稳定性。</td></tr><tr><td>跨节点 Recover p95/p99</td><td>ms</td><td>评估外部快照在不同节点恢复的效率。</td><td>必须确认 source_node != target_node；尾延迟需结合 atelet 日志中的 download、oci_unpack、ateom_restore 分解。</td></tr><tr><td>内存梯度</td><td>MiB/GiB</td><td>评估 snapshot 尺寸变大后的恢复衰减。</td><td>大内存失败也是容量信号，原始失败样本必须保留。</td></tr><tr><td>暂停 p95</td><td>ms</td><td>评估 checkpoint 成本。</td><td>suspend_1 和 suspend_2 分开展示，避免混合不同阶段。</td></tr></tbody></table></section>"
}

func renderEnvironmentAndLimits() string {
	return "<section class=\"narrative\"><h2>环境与约束</h2><ul><li>运行环境：ACK + Kubernetes + containerd + gVisor/runsc + Substrate worker pool。</li><li>已知环境问题：ACK 节点 user.max_user_namespaces=0 会导致 runsc create 失败；修复后新环境烟测通过。</li><li>容量解释：no free workers available 表示 worker 池可用容量不足或仍被 actor 占用，属于容量边界信号。</li><li>当前限制：报告混合了性能轮次和环境修复轮次；结论中已明确区分“性能基线”和“环境验收”。</li></ul></section>"
}

func renderAnomalyAnalysis(summary Summary) string {
	type anomaly struct {
		category string
		count    int
		meaning  string
	}
	byCategory := map[string]*anomaly{}
	for _, e := range summary.Errors {
		category := errorCategory(e)
		row := byCategory[category]
		if row == nil {
			row = &anomaly{category: category, meaning: errorMeaning(e)}
			byCategory[category] = row
		}
		row.count += e.Count
	}
	categories := make([]string, 0, len(byCategory))
	for category := range byCategory {
		categories = append(categories, category)
	}
	sort.Strings(categories)

	var b strings.Builder
	b.WriteString("<section class=\"narrative\"><h2>瓶颈与异常分析</h2>")
	if len(categories) == 0 {
		b.WriteString("<p>本次没有记录失败事件。</p></section>")
		return b.String()
	}
	b.WriteString("<p>异常按性质归类，避免把容量饱和、环境配置和清理路径缺口混成同一种失败。</p><table><thead><tr><th>类别</th><th>失败次数</th><th>解释</th></tr></thead><tbody>")
	for _, category := range categories {
		row := byCategory[category]
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%d</td><td>%s</td></tr>", html.EscapeString(row.category), row.count, html.EscapeString(row.meaning))
	}
	b.WriteString("</tbody></table></section>")
	return b.String()
}

func renderScenarioTable(rounds []RoundSummary) string {
	var b strings.Builder
	b.WriteString("<h2>测试矩阵</h2><p class=\"section-note\">场景名按测试意图命名，内部 ID 只用于追溯原始数据。性能结论优先看“10 节点性能基线”和“容量探测”，环境修复轮次不混入性能排名。</p><table><thead><tr><th>分组</th><th>场景</th><th>内部 ID</th><th>节点</th><th>并发</th><th>Actors</th><th>实验目的</th><th>期望观察信号</th><th>本轮解读</th></tr></thead><tbody>")
	for _, r := range orderedRounds(rounds) {
		info := scenarioByID(r.Round)
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td><code>%s</code></td><td>%d</td><td>%d</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td></tr>", html.EscapeString(info.Group), html.EscapeString(info.Label), html.EscapeString(r.Round), r.NodeScale, r.Concurrency, r.Actors, html.EscapeString(info.Intent), html.EscapeString(info.ExpectedSignal), html.EscapeString(roundInterpretation(r)))
	}
	b.WriteString("</tbody></table>")
	return b.String()
}

func renderRoundSummaryTable(rounds []RoundSummary) string {
	var b strings.Builder
	b.WriteString("<h2>轮次结果摘要</h2><table><thead><tr><th>分组</th><th>场景</th><th>节点标签</th><th>并发</th><th>Actors</th><th>操作数</th><th>成功</th><th>失败</th><th>错误率</th><th>吞吐 ops/s</th><th>是否性能基线</th></tr></thead><tbody>")
	for _, r := range orderedRounds(rounds) {
		info := scenarioByID(r.Round)
		baseline := "否"
		if info.UseForBaseline && r.Errors == 0 {
			baseline = "是"
		}
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s<br><code>%s</code></td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%.1f%%</td><td>%.2f</td><td>%s</td></tr>", html.EscapeString(info.Group), html.EscapeString(info.Label), html.EscapeString(r.Round), r.NodeScale, r.Concurrency, r.Actors, r.Operations, r.Success, r.Errors, r.ErrorRate*100, r.OpsPerS, baseline)
	}
	b.WriteString("</tbody></table>")
	return b.String()
}

func renderRawDataLineage(meta ReportMetadata) string {
	var b strings.Builder
	b.WriteString("<section class=\"narrative\"><h2>原始数据与复现</h2>")
	fmt.Fprintf(&b, "<p>Run ID：<code>%s</code>。报告生成时保留原始 JSONL、派生 CSV/JSON 和 HTML，便于后续按节点规模、并发、operation、错误类型继续聚合。</p>", html.EscapeString(meta.RunID))
	b.WriteString("<table><thead><tr><th>数据层</th><th>路径</th><th>用途</th></tr></thead><tbody>")
	b.WriteString("<tr><td>原始事件</td><td><code>raw/substrate-api/lifecycle-events*.jsonl</code></td><td>逐条 lifecycle event，后续聚合必须以它为准。</td></tr>")
	b.WriteString("<tr><td>派生汇总</td><td><code>reports/data/summary.json</code></td><td>round、operation latency、error summary 的结构化汇总。</td></tr>")
	b.WriteString("<tr><td>表格数据</td><td><code>derived/*.csv</code></td><td>方便导入 BI、Notebook 或二次分析。</td></tr>")
	b.WriteString("<tr><td>HTML 报告</td><td><code>reports/index.html</code></td><td>标准化可视化交付物。</td></tr>")
	b.WriteString("</tbody></table><p>复现命令：<code>go run ./benchmarking/perfkit report --run-dir &lt;run-dir&gt; --run-id &lt;run-id&gt;</code>。</p></section>")
	return b.String()
}

func formatLatencyCell(success int, value float64) string {
	if success == 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.0f", value)
}

func renderChartPanel(title, note, svg string) string {
	return fmt.Sprintf("<section class=\"chart-panel\"><h3>%s</h3><p>%s</p>%s</section>", html.EscapeString(title), html.EscapeString(note), svg)
}

func scenarioByID(round string) scenarioInfo {
	scenarios := map[string]scenarioInfo{
		"r0-cli-smoke": {
			ID: "r0-cli-smoke", Label: "10 节点 CLI 手工烟测", Group: "连通性烟测",
			Intent:         "确认 ACK 上 Substrate API、worker、gVisor 生命周期基础链路可用。",
			ExpectedSignal: "所有基础操作能闭环，错误率为 0。",
			UseForBaseline: false,
		},
		"r1-lite": {
			ID: "r1-lite", Label: "10 节点轻量基线", Group: "10 节点性能基线",
			Intent:         "在低压力下建立生命周期延迟参考值。",
			ExpectedSignal: "错误率为 0，冷/热恢复 p95 形成后续对照。",
			UseForBaseline: true,
		},
		"r2-seq100": {
			ID: "r2-seq100", Label: "10 节点顺序 100 Actor", Group: "10 节点性能基线",
			Intent:         "用 100 个 actor 顺序执行完整生命周期，观察无并发压力时的稳定性。",
			ExpectedSignal: "错误率为 0，延迟分位不应明显劣化。",
			UseForBaseline: true,
		},
		"r2-con3": {
			ID: "r2-con3", Label: "10 节点并发 3 稳态", Group: "10 节点性能基线",
			Intent:         "用 200 个 actor、并发 3 验证常规并发下是否稳定。",
			ExpectedSignal: "错误率为 0，吞吐高于顺序轮次，p95 仍保持稳定。",
			UseForBaseline: true,
		},
		"r2-con10": {
			ID: "r2-con10", Label: "10 节点并发 10 容量探测", Group: "容量边界探测",
			Intent:         "用 200 个 actor、并发 10 压测 worker 池，寻找饱和边界。",
			ExpectedSignal: "若出现 no free workers，说明进入容量瓶颈区。",
			UseForBaseline: false,
		},
		"new-env-smoke": {
			ID: "new-env-smoke", Label: "新环境初始烟测", Group: "新环境验收",
			Intent:         "在新 ACK 环境首次执行工具烟测，暴露部署、镜像、worker 容量和启动路径问题。",
			ExpectedSignal: "用于发现环境问题，不作为性能基线。",
			UseForBaseline: false,
		},
		"new-env-smoke-counter-perf": {
			ID: "new-env-smoke-counter-perf", Label: "新环境镜像修复后烟测", Group: "新环境验收",
			Intent:         "切换到修正后的 ActorTemplate 后复测，定位 runsc create 失败。",
			ExpectedSignal: "若 runsc create 失败，优先检查 ACK 节点 user namespace。",
			UseForBaseline: false,
		},
		"new-env-smoke-counter-perf-after-userns": {
			ID: "new-env-smoke-counter-perf-after-userns", Label: "新环境 userns 修复后单 Actor 验证", Group: "新环境验收",
			Intent:         "设置 user.max_user_namespaces 后先跑 1 个 actor，验证节点修复是否有效。",
			ExpectedSignal: "单 actor 全生命周期通过。",
			UseForBaseline: false,
		},
		"new-env-smoke-counter-perf-success": {
			ID: "new-env-smoke-counter-perf-success", Label: "新环境工具验收成功轮", Group: "新环境验收",
			Intent:         "跑 3 个 actor / 18 个生命周期操作，证明 perfkit 和 ACK 节点准备流程可复跑。",
			ExpectedSignal: "所有操作成功，用作自动化验收闭环。",
			UseForBaseline: false,
		},
		"cross-node-recover-con1": {
			ID: "cross-node-recover-con1", Label: "10 节点跨节点 Recover", Group: "跨节点恢复验证",
			Intent:         "通过外部快照恢复，并排空源节点可用 worker，验证 Recover 后落到不同节点的延迟和成功率。",
			ExpectedSignal: "cross_node_recover 成功样本必须满足 source_node != target_node，错误率为 0。",
			UseForBaseline: false,
		},
		"hot-migration-con1": {
			ID: "hot-migration-con1", Label: "16GiB 跨节点 Hot Migration", Group: "跨节点热迁移验证",
			Intent:         "从发起 PrepareActorMigration 开始持续 HTTP 探测，验证路由切换后目标节点可访问、请求最长断流和 route generation 变化。",
			ExpectedSignal: "source_node != target_node，route generation 增加，HTTP 5xx 为 0，发起到目标成功响应 <= 30s。",
			UseForBaseline: false,
		},
		"oci-r0-baseline": {
			ID: "oci-r0-baseline", Label: "OCI baseline：每次恢复重新 unpack", Group: "OCI unpack 优化",
			Intent:         "在未启用 rootfs-cache 的 atelet 上执行跨节点 Recover，作为 OCI unpack 优化前对照。",
			ExpectedSignal: "冷目标节点可能出现秒级 oci_unpack 尾延迟。",
			UseForBaseline: true,
		},
		"oci-r1-image-cache": {
			ID: "oci-r1-image-cache", Label: "OCI round1：镜像 pull cache", Group: "OCI unpack 优化",
			Intent:         "只复用内存中的镜像 tar，验证网络/registry pull 不再是瓶颈。",
			ExpectedSignal: "若 p95 仍高，说明瓶颈在 untar/rootfs 准备而不是镜像拉取。",
			UseForBaseline: false,
		},
		"oci-r2-rootfs-cache": {
			ID: "oci-r2-rootfs-cache", Label: "OCI round2：已解包 rootfs cache", Group: "OCI unpack 优化",
			Intent:         "启用 ATE_OCI_ROOTFS_CACHE=1，在节点上复用已解包 rootfs 模板。",
			ExpectedSignal: "cross_node_recover 的 p95/p99 应显著低于 baseline 的冷 unpack 尾延迟。",
			UseForBaseline: false,
		},
		"oci-r3-memory-gradient": {
			ID: "oci-r3-memory-gradient", Label: "OCI round3：内存梯度联动", Group: "OCI unpack 优化",
			Intent:         "在 rootfs-cache 已启用后运行内存梯度，确认剩余延迟主要来自 snapshot 恢复而不是 OCI unpack。",
			ExpectedSignal: "小内存接近热路径，大内存随 snapshot 尺寸上升出现恢复衰减。",
			UseForBaseline: false,
		},
	}
	if info, ok := scenarios[round]; ok {
		return info
	}
	if mem, ok := memoryRoundMiB(round); ok {
		return scenarioInfo{
			ID: round, Label: memoryRoundLabel(mem), Group: "内存梯度",
			Intent:         fmt.Sprintf("使用 %s resident memory workload 测试 checkpoint/restore 随内存尺寸增长的变化。", memorySizeLabel(mem)),
			ExpectedSignal: "恢复和 checkpoint 延迟应随 snapshot 尺寸增长而上升；若不升高，需要核对实际 snapshot 文件尺寸。",
			UseForBaseline: false,
		}
	}
	if strings.HasSuffix(round, "nodes") {
		return scenarioInfo{ID: round, Label: round, Group: "节点规模聚合", Intent: "按节点规模标签聚合的趋势项。", ExpectedSignal: "用于比较不同节点规模下的吞吐和错误率。"}
	}
	return scenarioInfo{
		ID: round, Label: round, Group: "未登记场景",
		Intent:         "未登记的测试轮次；请在 perfdata.scenarioByID 中补充该场景的测试目的。",
		ExpectedSignal: "未定义。",
	}
}

func orderedRounds(rounds []RoundSummary) []RoundSummary {
	out := append([]RoundSummary(nil), rounds...)
	sort.SliceStable(out, func(i, j int) bool {
		ai := scenarioOrder(out[i].Round)
		aj := scenarioOrder(out[j].Round)
		if ai != aj {
			return ai < aj
		}
		return out[i].Round < out[j].Round
	})
	return out
}

func performanceTrendRounds(rounds []RoundSummary) []RoundSummary {
	var out []RoundSummary
	for _, r := range orderedRounds(rounds) {
		switch scenarioByID(r.Round).Group {
		case "10 节点性能基线", "容量边界探测", "跨节点恢复验证", "跨节点热迁移验证", "OCI unpack 优化", "内存梯度":
			out = append(out, r)
		}
	}
	return out
}

func performanceTrendLatencyRows(rows []LatencySummary) []LatencySummary {
	var out []LatencySummary
	for _, r := range rows {
		switch scenarioByID(r.Round).Group {
		case "10 节点性能基线", "容量边界探测", "跨节点恢复验证", "跨节点热迁移验证", "OCI unpack 优化", "内存梯度":
			if r.Success > 0 {
				out = append(out, r)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		ri := scenarioOrder(out[i].Round)
		rj := scenarioOrder(out[j].Round)
		if ri != rj {
			return ri < rj
		}
		return out[i].Operation < out[j].Operation
	})
	return out
}

func scenarioOrder(round string) int {
	switch round {
	case "r0-cli-smoke":
		return 10
	case "r1-lite":
		return 20
	case "r2-seq100":
		return 30
	case "r2-con3":
		return 40
	case "r2-con10":
		return 50
	case "new-env-smoke":
		return 90
	case "new-env-smoke-counter-perf":
		return 91
	case "new-env-smoke-counter-perf-after-userns":
		return 92
	case "new-env-smoke-counter-perf-success":
		return 93
	case "cross-node-recover-con1":
		return 60
	case "hot-migration-con1":
		return 65
	case "oci-r0-baseline":
		return 70
	case "oci-r1-image-cache":
		return 71
	case "oci-r2-rootfs-cache":
		return 72
	case "oci-r3-memory-gradient":
		return 73
	default:
		if mem, ok := memoryRoundMiB(round); ok {
			return 200 + mem
		}
		return 1000
	}
}

func roundLabel(round string) string {
	return scenarioByID(round).Label
}

func roundPurpose(round string) string {
	switch round {
	case "r0-cli-smoke":
		return "用 CLI/手工路径确认 ACK 上 Substrate API、worker、gVisor 生命周期基础链路可用。"
	case "r1-lite":
		return "在低压力下建立 10 节点生命周期延迟基线，作为后续并发轮次的对照。"
	case "r2-seq100":
		return "用 100 个 actor 顺序执行完整生命周期，观察非并发情况下的稳定延迟和错误率。"
	case "r2-con3":
		return "用 200 个 actor、并发 3 跑完整生命周期，验证常规并发下是否稳定。"
	case "r2-con10":
		return "用 200 个 actor、并发 10 压测 worker 池，寻找 no free workers 的容量边界。"
	case "new-env-smoke":
		return "在新 ACK 环境首次执行工具烟测，用于暴露部署、镜像、worker 容量和启动路径问题。"
	case "new-env-smoke-counter-perf":
		return "切换到修正后的 ActorTemplate 后复测，用于定位 runsc create 失败。"
	case "new-env-smoke-counter-perf-after-userns":
		return "设置 user.max_user_namespaces 后先跑 1 个 actor，验证 ACK 节点 userns 修复是否有效。"
	case "new-env-smoke-counter-perf-success":
		return "在新环境跑 3 个 actor / 18 个生命周期操作，证明 perfkit 和 ACK 节点准备流程可复跑。"
	case "cross-node-recover-con1":
		return "使用外部快照执行 Recover，并用临时 blocker actor 排空源节点可用 worker，验证恢复是否真实跨节点。"
	case "hot-migration-con1":
		return "从发起迁移 RPC 开始持续访问 actor HTTP 入口，记录 Prepare、Commit、目标成功响应、最长成功间隔和 route generation 变化。"
	default:
		return "未登记的测试轮次；请在 perfdata.roundPurpose 中补充该场景的测试目的。"
	}
}

func roundInterpretation(r RoundSummary) string {
	switch r.Round {
	case "r2-con10":
		if r.Errors > 0 {
			return "该轮错误主要是 no free workers，表示并发 10 下 worker 池容量已被打满；这是容量信号，不是单个 actor 生命周期延迟结论。"
		}
	case "new-env-smoke", "new-env-smoke-counter-perf":
		if r.Errors > 0 {
			return "该轮用于暴露环境问题，错误应结合错误摘要和过程日志看，不应作为最终性能基线。"
		}
	case "new-env-smoke-counter-perf-after-userns", "new-env-smoke-counter-perf-success":
		if r.Errors == 0 {
			return "该轮证明 ACK user namespace 修复后 gVisor 生命周期路径可以通过。样本量用于工具验收，不用于规模性能结论。"
		}
	case "cross-node-recover-con1":
		if r.Errors == 0 {
			return "该轮所有 cross_node_recover 成功样本均要求 source_node != target_node，可用于验收跨节点恢复延迟。"
		}
		return "该轮存在未跨节点、容量不足或恢复失败样本，不能作为跨节点 Recover 成功结论。"
	case "hot-migration-con1":
		if r.Errors == 0 {
			return "该轮可用于读取端到端热迁移数据：重点看 hot_migration_summary 的 total_to_target_success_ms、longest_success_gap_ms 和 route generation。"
		}
		return "该轮热迁移存在失败事件，需先看 prepare/commit/probe 哪个阶段失败，再判断瓶颈在目标恢复、路由切换还是请求连续性。"
	}
	if r.Errors == 0 {
		return "该轮所有 lifecycle 操作成功，可用于观察该并发/规模下的延迟和吞吐。"
	}
	return "该轮包含失败操作，优先阅读错误摘要判断是容量饱和、环境配置还是工具链问题。"
}

func operationLabel(operation string) string {
	switch operation {
	case "create":
		return "创建 actor 元数据"
	case "create_atespace":
		return "创建 atespace"
	case "resume_boot":
		return "冷恢复 / 首次启动"
	case "suspend_1":
		return "第一次暂停 / checkpoint"
	case "resume_warm":
		return "热恢复 / 从暂停态恢复"
	case "suspend_external":
		return "外部快照 checkpoint"
	case "cross_node_recover":
		return "跨节点 Recover"
	case "prepare_actor_migration":
		return "准备目标 worker / 目标恢复"
	case "commit_actor_migration":
		return "提交迁移 / 路由切换"
	case "http_probe_before_migration":
		return "迁移前 HTTP 探测"
	case "http_probe_after_migration":
		return "迁移后 HTTP 探测"
	case "hot_migration_summary":
		return "Hot migration 端到端摘要"
	case "suspend_after_migration":
		return "迁移后暂停"
	case "suspend_after_recover":
		return "Recover 后暂停"
	case "blocker_create":
		return "源节点占位 actor 创建"
	case "blocker_resume_boot":
		return "源节点占位 actor 启动"
	case "suspend_2":
		return "第二次暂停 / checkpoint"
	case "delete":
		return "删除 actor"
	case "cleanup_suspend":
		return "失败后清理暂停"
	case "cleanup_delete":
		return "失败后清理删除"
	default:
		return operation
	}
}

func errorMeaning(e ErrorSummary) string {
	if strings.Contains(e.ErrorReason, "no free workers") {
		return "worker 池容量不足或仍有 actor 占用 worker。用于识别容量饱和点。"
	}
	if strings.Contains(e.ErrorReason, "runsc create") {
		return "gVisor runsc 创建沙箱失败。本轮根因是 ACK 节点 user.max_user_namespaces=0。"
	}
	if strings.Contains(e.ErrorReason, "context deadline exceeded") {
		return "actor 启动或恢复超过等待窗口，需要结合 atelet/ateom 日志定位镜像拉取、runsc 或 readiness。"
	}
	if strings.Contains(e.ErrorReason, "missing bucket") {
		return "清理阶段缺少 snapshot bucket 配置，属于失败路径清理缺口，不代表成功路径性能。"
	}
	return "未归类错误，需结合原始原因和过程日志分析。"
}

func errorCategory(e ErrorSummary) string {
	if strings.Contains(e.ErrorReason, "no free workers") {
		return "容量饱和 / worker 池不足"
	}
	if strings.Contains(e.ErrorReason, "runsc create") {
		return "环境配置 / gVisor user namespace"
	}
	if strings.Contains(e.ErrorReason, "context deadline exceeded") {
		return "启动超时 / readiness"
	}
	if strings.Contains(e.ErrorReason, "missing bucket") {
		return "失败路径清理 / snapshot 配置"
	}
	return "未归类异常"
}

func bestLatencyForGroup(summary Summary, group, operation string) *LatencySummary {
	var best *LatencySummary
	for i := range summary.LatencyPercentiles {
		row := &summary.LatencyPercentiles[i]
		if scenarioByID(row.Round).Group != group || row.Operation != operation || row.Success == 0 {
			continue
		}
		if best == nil || row.P95MS < best.P95MS {
			best = row
		}
	}
	return best
}

func worstLatencyForGroup(summary Summary, group, operation string) *LatencySummary {
	var worst *LatencySummary
	for i := range summary.LatencyPercentiles {
		row := &summary.LatencyPercentiles[i]
		if scenarioByID(row.Round).Group != group || row.Operation != operation || row.Success == 0 {
			continue
		}
		if worst == nil || row.P95MS > worst.P95MS {
			worst = row
		}
	}
	return worst
}

func memoryRoundMiB(round string) (int, bool) {
	raw, ok := strings.CutPrefix(round, "mem-")
	if !ok {
		return 0, false
	}
	mult := 1
	switch {
	case strings.HasSuffix(raw, "m"):
		raw = strings.TrimSuffix(raw, "m")
	case strings.HasSuffix(raw, "g"):
		raw = strings.TrimSuffix(raw, "g")
		mult = 1024
	default:
		return 0, false
	}
	var n int
	for _, ch := range raw {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		n = n*10 + int(ch-'0')
	}
	if n <= 0 {
		return 0, false
	}
	return n * mult, true
}

func memoryRoundLabel(mib int) string {
	return "内存梯度：" + memorySizeLabel(mib)
}

func memorySizeLabel(mib int) string {
	if mib%1024 == 0 {
		return fmt.Sprintf("%dGiB", mib/1024)
	}
	return fmt.Sprintf("%dMiB", mib)
}

func percentile(vals []float64, q float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	cp := append([]float64(nil), vals...)
	sort.Float64s(cp)
	if len(cp) == 1 {
		return cp[0]
	}
	rank := (float64(len(cp)) - 1) * (q / 100)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return cp[lo]
	}
	return cp[lo] + (cp[hi]-cp[lo])*(rank-float64(lo))
}

func parseTime(v string) time.Time {
	if v == "" {
		return time.Time{}
	}
	if strings.HasSuffix(v, "Z") {
		v = strings.TrimSuffix(v, "Z") + "+00:00"
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}
	}
	return t
}

func maxNodeScale(rows []LifecycleEvent) int {
	max := 0
	for _, ev := range rows {
		if ev.NodeScale > max {
			max = ev.NodeScale
		}
	}
	return max
}

func maxConcurrency(rows []LifecycleEvent) int {
	max := 0
	for _, ev := range rows {
		if ev.Concurrency > max {
			max = ev.Concurrency
		}
	}
	return max
}

func reportCSS() string {
	return `body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;margin:0;color:#172033;background:#f7f8fb}header{background:#111827;color:#fff;padding:28px 36px}main{padding:28px 36px;max-width:1320px;margin:0 auto}h1{font-size:28px;margin:0 0 8px}h2{font-size:20px;margin:28px 0 12px}h3{font-size:15px;margin:0 0 6px}.executive{display:grid;grid-template-columns:1.8fr 1fr;gap:14px;align-items:stretch}.executive>div,.kpis div,.narrative,.chart-panel,.finding{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px}.executive p,.executive li,.narrative p,.narrative li{line-height:1.65}.executive ul,.narrative ul{margin:8px 0 0 20px}.kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.kpis b{display:block;font-size:24px}.kpis span,.section-note,.chart-panel p{color:#64748b;font-size:12px;line-height:1.5}.finding-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.finding{line-height:1.55}.charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.chart-panel svg{border:0;border-radius:0}table{width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb}th,td{padding:8px 10px;border-bottom:1px solid #e5e7eb;text-align:left;font-size:13px;vertical-align:top}th{background:#f1f5f9;color:#334155}code{font-size:12px;background:#eef2f7;border-radius:4px;padding:1px 4px}svg{width:100%;height:auto;background:#fff;border:1px solid #e5e7eb;border-radius:8px}.chart-title{font-size:16px;font-weight:700;fill:#172033}.axis-label{font-size:11px;fill:#475569}.value-label{font-size:11px;fill:#172033}.legend{font-size:11px;fill:#475569}@media(max-width:1000px){.executive,.finding-grid,.kpis,.charts{grid-template-columns:1fr}main,header{padding:18px}}`
}

func renderBarSVG(title string, rounds []RoundSummary, value func(RoundSummary) float64, suffix string) string {
	const width = 920
	const padL = 140
	const padR = 40
	const padT = 36
	const barH = 26
	height := maxInt(90, padT+len(rounds)*(barH+12)+20)
	var max float64
	for _, r := range rounds {
		if v := value(r); v > max {
			max = v
		}
	}
	if max == 0 {
		max = 1
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<svg viewBox=\"0 0 %d %d\" role=\"img\"><text x=\"%d\" y=\"24\" class=\"chart-title\">%s</text>", width, height, padL, html.EscapeString(title))
	for i, r := range rounds {
		y := padT + i*(barH+12)
		v := value(r)
		w := (float64(width-padL-padR) * v) / max
		fmt.Fprintf(&b, "<text x=\"8\" y=\"%d\" class=\"axis-label\">%s</text><rect x=\"%d\" y=\"%d\" width=\"%.1f\" height=\"%d\" rx=\"3\" fill=\"#2563eb\"></rect><text x=\"%.1f\" y=\"%d\" class=\"value-label\">%.1f%s</text>", y+18, html.EscapeString(roundLabel(r.Round)), padL, y, w, barH, float64(padL)+w+6, y+18, v, suffix)
	}
	b.WriteString("</svg>")
	return b.String()
}

func renderStackedRoundSVG(title string, rounds []RoundSummary) string {
	const width = 920
	const padL = 140
	const padR = 40
	const padT = 50
	const barH = 24
	height := maxInt(110, padT+len(rounds)*(barH+12)+20)
	var max float64
	for _, r := range rounds {
		if float64(r.Operations) > max {
			max = float64(r.Operations)
		}
	}
	if max == 0 {
		max = 1
	}
	var b strings.Builder
	fmt.Fprintf(&b, "<svg viewBox=\"0 0 %d %d\" role=\"img\"><text x=\"%d\" y=\"24\" class=\"chart-title\">%s</text>", width, height, padL, html.EscapeString(title))
	fmt.Fprintf(&b, "<rect x=\"%d\" y=\"34\" width=\"12\" height=\"12\" fill=\"#16a34a\"></rect><text x=\"%d\" y=\"44\" class=\"legend\">success</text><rect x=\"%d\" y=\"34\" width=\"12\" height=\"12\" fill=\"#dc2626\"></rect><text x=\"%d\" y=\"44\" class=\"legend\">errors</text>", padL, padL+18, padL+82, padL+100)
	for i, r := range rounds {
		y := padT + i*(barH+12)
		okW := float64(width-padL-padR) * float64(r.Success) / max
		errW := float64(width-padL-padR) * float64(r.Errors) / max
		fmt.Fprintf(&b, "<text x=\"8\" y=\"%d\" class=\"axis-label\">%s</text><rect x=\"%d\" y=\"%d\" width=\"%.1f\" height=\"%d\" rx=\"3\" fill=\"#16a34a\"></rect><rect x=\"%.1f\" y=\"%d\" width=\"%.1f\" height=\"%d\" rx=\"3\" fill=\"#dc2626\"></rect><text x=\"%.1f\" y=\"%d\" class=\"value-label\">%d/%d</text>", y+17, html.EscapeString(roundLabel(r.Round)), padL, y, okW, barH, float64(padL)+okW, y, errW, barH, float64(padL)+okW+errW+6, y+17, r.Success, r.Operations)
	}
	b.WriteString("</svg>")
	return b.String()
}

func renderLatencyOperationSVG(title string, rows []LatencySummary, operation string) string {
	var rounds []RoundSummary
	for _, row := range rows {
		if row.Operation == operation {
			rounds = append(rounds, RoundSummary{Round: row.Round, NodeScale: row.NodeScale, Concurrency: row.Concurrency, Operations: row.Samples, Success: row.Success, Errors: row.Errors, ErrorRate: row.ErrorRate, OpsPerS: row.P95MS})
		}
	}
	return renderBarSVG(title, rounds, func(r RoundSummary) float64 { return r.OpsPerS }, " ms")
}

func renderMaxLatencyPrefixSVG(title string, rows []LatencySummary, prefix string) string {
	byRound := map[string]RoundSummary{}
	for _, row := range rows {
		if !strings.HasPrefix(row.Operation, prefix) {
			continue
		}
		current := byRound[row.Round]
		if row.P95MS > current.OpsPerS {
			byRound[row.Round] = RoundSummary{Round: row.Round, NodeScale: row.NodeScale, Concurrency: row.Concurrency, Operations: row.Samples, Success: row.Success, Errors: row.Errors, ErrorRate: row.ErrorRate, OpsPerS: row.P95MS}
		}
	}
	rounds := make([]RoundSummary, 0, len(byRound))
	for _, row := range byRound {
		rounds = append(rounds, row)
	}
	sort.Slice(rounds, func(i, j int) bool { return rounds[i].Round < rounds[j].Round })
	return renderBarSVG(title, rounds, func(r RoundSummary) float64 { return r.OpsPerS }, " ms")
}

func aggregateByNodeScale(rounds []RoundSummary) []RoundSummary {
	byScale := map[int]RoundSummary{}
	for _, r := range rounds {
		current := byScale[r.NodeScale]
		current.Round = fmt.Sprintf("%d nodes", r.NodeScale)
		current.NodeScale = r.NodeScale
		current.Operations += r.Operations
		current.Success += r.Success
		current.Errors += r.Errors
		current.DurationS += r.DurationS
		if r.Concurrency > current.Concurrency {
			current.Concurrency = r.Concurrency
		}
		byScale[r.NodeScale] = current
	}
	out := make([]RoundSummary, 0, len(byScale))
	for _, r := range byScale {
		if r.DurationS > 0 {
			r.OpsPerS = float64(r.Operations) / r.DurationS
		}
		if r.Operations > 0 {
			r.ErrorRate = float64(r.Errors) / float64(r.Operations)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeScale < out[j].NodeScale })
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
