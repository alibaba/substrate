# Substrate 16GiB MicroVM 热迁移测试矩阵复现手册

生成时间：2026-07-27 10:15 CST

本文是测试方法文档，不是结果报告。目标是让专业测试人员可以按步骤复现 16GiB microVM 跨节点热迁移测试，并能独立判断每个场景是否通过。

## 1. 测试目标

验证 Substrate 在 16GiB microVM 场景下的跨节点迁移能力，重点关注：

| 目标 | 需要证明 |
|---|---|
| 用户请求连续性 | 迁移期间 HTTP GET、POST mutation、SSE、WebSocket 不失败 |
| 强一致性 | 已接受 mutation 不丢、不回退、不靠 GET cache 掩盖 |
| 跨节点迁移 | source node 与 target node 必须不同 |
| 用户体感 | 最大请求 gap、SSE gap、WebSocket echo latency 可量化 |
| 后端适配 | Lustre / JuiceFS / JindoFS 等后端在相同口径下可比较 |
| 资源闭环 | 测试结束后 actor、worker assignment、本地 checkpoint 残留可清理 |

本文把测试分成两层：

| 层级 | 用途 |
|---|---|
| 基础 L7 smoke 矩阵 | 已具备可执行脚本，覆盖三后端 x 五类轻量交互 |
| 生产级真实场景矩阵 | 部分已测，部分需要补工具或补场景，不允许用 smoke 结果替代 |

## 2. 环境要求

### 2.1 集群与节点

| 项 | 要求 |
|---|---|
| Kubernetes 集群 | ACK 测试集群，不能和线上业务资源混用清理脚本 |
| 地域 | 日本东京测试集群 |
| 节点 OS | Alibaba Cloud Linux 3 |
| 架构 | amd64 |
| 嵌套虚拟化 | 必须开启，用于 Kata + Cloud Hypervisor microVM |
| 节点数量 | 单 actor 热迁移至少 3 个 worker；并发矩阵按 N 扩容 |
| 节点规格 | 优先网络增强型、支持嵌套虚拟化的 4x/8x 规格 |
| 本地盘 | checkpoint/restore 路线需要本地 RAID；live migration 路线不强依赖 |

### 2.2 本地访问配置

默认使用 kubeconfig 直连 Kubernetes API：

```bash
export KUBECONFIG="<KUBECONFIG>"
```

所有 perfkit 测试使用 direct L7 proxy：

```bash
export PERFKIT_ROUTER_SERVICE_NAME=atenet-router-direct
```

### 2.3 基础工具

测试机需要具备：

```bash
go version
kubectl version --client
jq --version
python3 --version
```

测试命令均在仓库根目录执行。本文用 `<REPO_ROOT>` 表示 Substrate 仓库根目录：

```bash
cd <REPO_ROOT>
```

## 3. 被测组件配置

### 3.1 通用 live migration 参数

所有正式热迁移测试必须使用同一组底层参数，除非单独说明：

| 参数 | 值 |
|---|---|
| `--live-migration-memory-mode` | `Precopy` |
| `--live-migration-downtime-ms` | `100` |
| `--live-migration-connections` | `8` |
| `--require-cross-node` | `true` |
| `--boot-source` | 开启 |
| `--count` | `1`，单 actor 测试 |
| `--concurrency` | `1` |
| `--sse-path` | `/stream` |
| `--terminal-websocket-path` | `/terminal/ws`，terminal 场景启用 |
| `--probe-path` | `/substrate/migration-state` |
| `--mutation-path` | 默认 `/increment` |

### 3.2 通用 ActorTemplate 要求

16GiB 测试 workload 必须满足：

| 配置 | 值 |
|---|---|
| sandbox | `microvm` |
| 内存目标 | `TARGET_MEMORY_MIB=15360` |
| 内存模式 | `MEMORY_PATTERN=dense-random` |
| seed | `MEMORY_SEED=20260723` |
| readyz | HTTP `/readyz` |
| 基础状态接口 | `/substrate/migration-state` |
| mutation 接口 | `/increment` |
| SSE 接口 | `/stream` |
| terminal WebSocket | `/terminal/ws` |
| upload 接口 | `/upload?max_bytes=67108864` |
| download 接口 | `/download?bytes=...` |
| dirty memory 接口 | `/dirty/start?rate_mib_per_s=...` |

通用 16GiB terminal/SSE 模板示例：

```text
namespace: microvm-live-migration-20260723
name: generic-livech-memory-16g-sse-replay-20260726014330-ttl
worker selector: workload=livech-microvm-16g
```

三后端矩阵模板：

| 后端 | namespace | ActorTemplate | worker label |
|---|---|---|---|
| Lustre | `microvm-memory-lustre-16g` | `memory-microvm-16g-realmatrix-20260725040842` | `memory-microvm-16g-lustre` |
| JuiceFS | `fluid-juicefs-tmpfs-poc` | `juicefs-livech-memory-16g-realmatrix-20260725034729` | `juicefs-livech-microvm-16g-no141` |
| JindoFS | `fluid-jindofs-livech-poc` | `jindo-livech-memory-16g-realmatrix` | `jindo-livech-microvm-16g-no141` |

## 4. 测试前检查

每轮测试前必须记录环境状态：

```bash
STAMP="$(date +%Y%m%d%H%M%S)"
OUT_ROOT="results/live-migration-evidence-20260724/repro-${STAMP}"
mkdir -p "${OUT_ROOT}"

kubectl get nodes -o wide > "${OUT_ROOT}/nodes.txt"
kubectl get pods -A -o wide > "${OUT_ROOT}/pods-all.txt"
go run ./cmd/kubectl-ate get workers -o json > "${OUT_ROOT}/workers-before.json"
go run ./cmd/kubectl-ate get actors -A -o json > "${OUT_ROOT}/actors-before.json"
kubectl -n ate-system get deploy,pods -o wide > "${OUT_ROOT}/ate-system.txt"
git rev-parse HEAD > "${OUT_ROOT}/git-head.txt"
git status --short > "${OUT_ROOT}/git-status.txt"
```

检查 worker 是否有足够空闲容量：

```bash
jq -r '.workers[] | [.workerNamespace, .workerPod, .nodeName, (.assignment // "free")] | @tsv' \
  "${OUT_ROOT}/workers-before.json"
```

正式单 actor 测试至少要求目标 workerpool 内有 2 个以上可用 worker，且 source 与 target 可以落到不同节点。

## 5. 标准执行流程

每个测试 cell 都按以下步骤执行：

1. 创建唯一 `ATESPACE` 和输出目录。
2. 执行 pre-cleanup，确保同名 atespace 没有历史残留。
3. 记录 workers、actors、pods 初始状态。
4. 执行 `perfkit run-hot-migration`。
5. 保存 JSONL、perfkit.log、exit-code。
6. 用 jq 校验 `hot_migration_summary`、`sse_probe_summary`、`terminal_websocket_probe_summary`。
7. 执行 post-cleanup。
8. 再次记录 workers、actors、pods。
9. 对输出目录生成 SHA256。

标准目录结构：

```text
results/live-migration-evidence-20260724/repro-<timestamp>/
  <cell-name>/
    command.sh
    perfkit.log
    <cell-name>.jsonl
    exit-code.txt
    workers-before.json
    actors-before.json
    pods-before.txt
    post-cleanup.jsonl
    workers-after.json
    actors-after.json
    pods-after.txt
    SHA256SUMS.txt
```

## 6. 单 actor 用户窗口热迁移复现

这是当前最核心的热迁移验收场景。它验证 HTTP、POST、SSE、WebSocket terminal 在迁移窗口内是否连续。

```bash
STAMP="$(date +%Y%m%d%H%M%S)"
OUT_DIR="results/live-migration-evidence-20260724/repro-${STAMP}/user-window"
ATESPACE="repro-user-window-${STAMP}"
mkdir -p "${OUT_DIR}"

cat > "${OUT_DIR}/command.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

export PERFKIT_ROUTER_SERVICE_NAME=atenet-router-direct

OUT_DIR="${OUT_DIR:?}"
ATESPACE="${ATESPACE:?}"
KUBECONFIG="${KUBECONFIG:?set KUBECONFIG to <KUBECONFIG>}"

go run ./benchmarking/perfkit cleanup-atespace \
  --kubeconfig "${KUBECONFIG}" \
  --atespace "${ATESPACE}" \
  --concurrency 4 \
  --out "${OUT_DIR}/pre-cleanup.jsonl" \
  2>&1 | tee "${OUT_DIR}/pre-cleanup.log" || true

go run ./cmd/kubectl-ate get workers -o json > "${OUT_DIR}/workers-before.json"
go run ./cmd/kubectl-ate get actors -A -o json > "${OUT_DIR}/actors-before.json"
kubectl -n microvm-live-migration-20260723 get pods -o wide > "${OUT_DIR}/pods-before.txt"

set +e
go run ./benchmarking/perfkit run-hot-migration \
  --kubeconfig "${KUBECONFIG}" \
  --atespace "${ATESPACE}" \
  --run-id "${ATESPACE}" \
  --round "${ATESPACE}" \
  --stage repro_user_window \
  --workload livech-memory-16g-user-window-terminal-sse \
  --actor-prefix ruw16 \
  --actor-template-namespace microvm-live-migration-20260723 \
  --actor-template-name generic-livech-memory-16g-sse-replay-20260726014330-ttl \
  --boot-source \
  --node-scale 3 \
  --count 1 \
  --concurrency 1 \
  --probe-interval 100ms \
  --post-commit-probe-timeout 180s \
  --cleanup-isolation-duration 60s \
  --migration-rpc-timeout 240s \
  --live-migration-downtime-ms 100 \
  --live-migration-connections 8 \
  --live-migration-memory-mode Precopy \
  --require-route-switched-probe \
  --state-mutations 1 \
  --continuous-mutation-interval 100ms \
  --sse-path /stream \
  --terminal-websocket-path /terminal/ws \
  --require-cross-node=true \
  --skip-final-actor-cleanup \
  --out "${OUT_DIR}/${ATESPACE}.jsonl" \
  2>&1 | tee "${OUT_DIR}/perfkit.log"
RUN_STATUS=${PIPESTATUS[0]}
set -e
echo "${RUN_STATUS}" > "${OUT_DIR}/exit-code.txt"

go run ./benchmarking/perfkit cleanup-atespace \
  --kubeconfig "${KUBECONFIG}" \
  --atespace "${ATESPACE}" \
  --concurrency 4 \
  --out "${OUT_DIR}/post-cleanup.jsonl" \
  2>&1 | tee "${OUT_DIR}/post-cleanup.log" || true

go run ./cmd/kubectl-ate get workers -o json > "${OUT_DIR}/workers-after.json"
go run ./cmd/kubectl-ate get actors -A -o json > "${OUT_DIR}/actors-after.json"
kubectl -n microvm-live-migration-20260723 get pods -o wide > "${OUT_DIR}/pods-after.txt"
shasum -a 256 "${OUT_DIR}"/* > "${OUT_DIR}/SHA256SUMS.txt" || true

exit "${RUN_STATUS}"
SH

OUT_DIR="${OUT_DIR}" ATESPACE="${ATESPACE}" bash "${OUT_DIR}/command.sh"
```

### 验收命令

```bash
JSONL="${OUT_DIR}/${ATESPACE}.jsonl"

jq -r 'select(.operation=="hot_migration_summary") | {
  result,
  migration_window_result,
  cross_node,
  total_to_target_success_ms,
  commit_live_migration_ms: .stage_duration_ms.commit_live_migration,
  probe_requests,
  probe_successes,
  mutation_requests,
  mutation_successes,
  route_switched_probe_requests,
  route_switched_probe_successes,
  service_errors_during_route_switched,
  mutation_route_switched_probe_requests,
  mutation_route_switched_probe_successes,
  mutation_service_errors_during_route_switched,
  stream_max_message_gap_ms,
  terminal_max_message_gap_ms,
  terminal_echo_latency_p95_ms,
  terminal_echo_latency_max_ms,
  terminal_sequence_gap_count,
  terminal_gap_threshold_violation
}' "${JSONL}"

jq -r 'select(.operation=="sse_probe_summary")' "${JSONL}"
jq -r 'select(.operation=="terminal_websocket_probe_summary")' "${JSONL}"
```

通过标准：

| 字段 | 通过条件 |
|---|---|
| `result` | `ok` |
| `migration_window_result` | `ok` |
| `cross_node` | `true` |
| `probe_successes == probe_requests` | 必须成立 |
| `mutation_successes == mutation_requests` | 必须成立 |
| `service_errors_during_route_switched` | `0` |
| `mutation_service_errors_during_route_switched` | `0` |
| `route_switched_probe_successes` | `> 0` |
| `mutation_route_switched_probe_successes` | `> 0` |
| `terminal_websocket_probe_summary.result` | `ok` |
| `terminal_sequence_gap_count` | `0` |
| `terminal_gap_threshold_violation` | `false` 或可解释 |

## 7. 三后端基础 L7 smoke 矩阵

该矩阵验证 Lustre / JuiceFS / JindoFS 在相同基础交互下是否都能完成热迁移。

### 7.1 矩阵定义

| 后端 | 场景 |
|---|---|
| Lustre | short / slow / upload / download / mixed |
| JuiceFS | short / slow / upload / download / mixed |
| JindoFS | short / slow / upload / download / mixed |

场景含义：

| 场景 | 请求组合 | 业务含义 | 边界 |
|---|---|---|---|
| short | GET `/substrate/migration-state` + POST `/increment` + SSE | 页面轮询和轻量状态写入 | 不代表长事务 |
| slow | short + GET `/slow?duration_ms=5000` | 5s 级慢接口跨迁移 | 不代表真实 notebook cell |
| upload | GET + SSE + POST `/upload?max_bytes=67108864`，body 64KiB | 小 payload 写入 | 不代表 1GiB 上传 |
| download | short + GET `/download?bytes=1048576&chunk_bytes=65536&chunk_delay_ms=2` | 1MiB 分块下载 | 不代表 1GiB 下载 |
| mixed | upload + slow + download + SSE | 单 actor 混合轻量请求 | 不代表高并发或多 actor |

### 7.2 标准脚本入口

已有标准脚本：

```bash
results/live-migration-evidence-20260724/run-real-scenario-matrix.sh
```

执行完整 15 cell：

```bash
export PERFKIT_ROUTER_SERVICE_NAME=atenet-router-direct
export OUT_ROOT="results/live-migration-evidence-20260724/repro-$(date +%Y%m%d%H%M%S)/runs-live-matrix"

bash results/live-migration-evidence-20260724/run-real-scenario-matrix.sh
```

只跑某个后端或某个场景：

```bash
BACKEND_FILTER=lustre SCENARIO_FILTER=short \
OUT_ROOT="results/live-migration-evidence-20260724/repro-$(date +%Y%m%d%H%M%S)/runs-live-matrix" \
bash results/live-migration-evidence-20260724/run-real-scenario-matrix.sh
```

### 7.3 脚本内关键配置

后端配置：

```text
lustre  | microvm-memory-lustre-16g | memory-microvm-16g-realmatrix-20260725040842 | memory-microvm-16g-lustre
juicefs | fluid-juicefs-tmpfs-poc   | juicefs-livech-memory-16g-realmatrix-20260725034729 | juicefs-livech-microvm-16g-no141
jindofs | fluid-jindofs-livech-poc  | jindo-livech-memory-16g-realmatrix | jindo-livech-microvm-16g-no141
```

场景参数：

| 场景 | 关键参数 |
|---|---|
| short | `--probe-interval=100ms`，`--continuous-mutation-interval=100ms` |
| slow | `--probe-interval=250ms`，`--extra-probe-paths=slow=/slow?duration_ms=5000` |
| upload | `--mutation-path=/upload?max_bytes=67108864`，`--mutation-body-bytes=65536`，`--continuous-mutation-interval=500ms` |
| download | `--extra-probe-paths=download=/download?bytes=1048576&chunk_bytes=65536&chunk_delay_ms=2`，`--probe-read-limit-bytes=2097152` |
| mixed | upload + slow + download 参数叠加 |

### 7.4 三后端矩阵验收

每个 cell 必须检查：

```bash
JSONL="<cell-dir>/<backend>-<scenario>.jsonl"

jq -r 'select(.operation=="hot_migration_summary") | [
  .result,
  .cross_node,
  .total_to_target_success_ms,
  .stage_duration_ms.commit_live_migration,
  "\(.probe_successes)/\(.probe_requests)",
  "\(.mutation_successes)/\(.mutation_requests)",
  .service_errors_during_route_switched,
  .stream_max_message_gap_ms
] | @tsv' "${JSONL}"

jq -r 'select(.operation=="extra_probe_summary")' "${JSONL}"
jq -r 'select(.operation=="sse_probe_summary")' "${JSONL}"
```

通过标准：

| 指标 | 标准 |
|---|---|
| cell exit-code | `0` |
| `hot_migration_summary.result` | `ok` |
| `cross_node` | `true` |
| GET | `probe_successes == probe_requests` |
| POST | `mutation_successes == mutation_requests` |
| route-switched service error | `0` |
| SSE | 无 sequence gap，最大 gap 有记录 |
| extra probe | slow/download/mixed 场景必须有对应 summary 且成功 |

## 8. 强一致 mutation 矩阵

强一致矩阵用于避免“GET cache 或后续全局状态快照掩盖 mutation 问题”。

### 8.1 必要 workload 行为

POST `/increment` 必须返回本次 mutation 自己提交后的：

| 字段 | 含义 |
|---|---|
| `counter` | 本次提交后的计数 |
| `state_version` | 本次提交后的状态版本 |
| `last_mutation_id` | 本次请求的 idempotency key |

测试客户端必须为每个 POST 设置唯一 `Idempotency-Key`，并在最终 summary 中校验 accepted mutation 没有丢失或回退。

### 8.2 执行方式

可以复用第 7 节三后端矩阵脚本，但必须确认 ActorTemplate 使用的是 commit-proof/idempotent workload 镜像。参考模板目录：

```text
results/live-migration-evidence-20260724/build/memory-commit-proof-livech-20260724235221/
```

### 8.3 验收字段

```bash
jq -r 'select(.operation=="hot_migration_summary") | {
  result,
  mutation_requests,
  mutation_successes,
  mutation_max_accepted_counter,
  http_counter,
  http_state_version,
  http_last_mutation_id,
  mutation_route_switched_probe_requests,
  mutation_route_switched_probe_successes,
  mutation_service_errors_during_route_switched
}' "${JSONL}"
```

通过标准：

| 项 | 标准 |
|---|---|
| mutation 请求 | 成功数等于请求数 |
| accepted mutation | 不丢失、不回退 |
| duplicate | 不允许不可解释重复提交 |
| route switch 期间 POST | 有成功样本，service error 为 0 |
| final state | counter/state_version/last_mutation_id 与 accepted mutation 对账一致 |

## 9. WebSocket terminal / PTY 场景

### 9.1 业务含义

模拟用户打开浏览器 terminal 或 code-server terminal，在迁移期间持续收 stdout，并持续发送输入 echo。

### 9.2 执行命令

使用第 6 节用户窗口命令，必须保留：

```bash
--terminal-websocket-path /terminal/ws
--sse-path /stream
--probe-interval 100ms
--continuous-mutation-interval 100ms
```

建议每个后端至少重复 5 次：

```bash
for i in 1 2 3 4 5; do
  # 每轮使用新的 ATESPACE 和 OUT_DIR，执行第 6 节 command.sh
  :
done
```

### 9.3 验收字段

```bash
jq -r 'select(.operation=="terminal_websocket_probe_summary") | {
  result,
  messages,
  echo_sent,
  echo_received,
  disconnects,
  missing_echo,
  duplicate_echo,
  sequence_gap_count,
  max_message_gap_ms,
  echo_latency_p95_ms,
  echo_latency_max_ms
}' "${JSONL}"
```

通过标准：

| 指标 | 标准 |
|---|---|
| WebSocket close/RST | 0 |
| echo | received == sent |
| missing echo | 0 |
| duplicate echo | 0 |
| sequence gap | 0 |
| PID / session identity | 不变化 |
| max gap | 必须记录；小于 1000ms 可称 UX ok，大于 1000ms 只能称 continuity ok |

## 10. Dirty memory 场景

### 10.1 矩阵

| 档位 | 参数 |
|---|---|
| idle | 不调用 dirty hook |
| low | `/dirty/start?rate_mib_per_s=128` |
| medium | `/dirty/start?rate_mib_per_s=512` |
| high | `/dirty/start?rate_mib_per_s=1024` |
| pathological | 尽力改写，需 workload 明确支持 |

每档建议至少 5 次，生产验收建议每档 10 次以上。

### 10.2 执行命令

以 1024MiB/s 为例：

```bash
--pre-migration-post-paths 'dirty=/dirty/start?rate_mib_per_s=1024'
--mutation-path /increment
--continuous-mutation-interval 100ms
--sse-path /stream
--terminal-websocket-path /terminal/ws
```

完整命令可参考已归档 dirty command：

```text
results/live-migration-evidence-20260724/runs-supplemental-20260726034313/generic-livech-dirty1024-s40-20260726034313/command.sh
```

### 10.3 验收字段

```bash
jq -r 'select(.operation=="hot_migration_summary") | {
  result,
  total_to_target_success_ms,
  commit_live_migration_ms: .stage_duration_ms.commit_live_migration,
  probe_requests,
  probe_successes,
  max_success_completion_gap_ms,
  max_single_request_latency_ms,
  stream_max_message_gap_ms,
  terminal_max_message_gap_ms,
  terminal_echo_latency_p95_ms,
  service_errors_during_route_switched
}' "${JSONL}"
```

通过标准：

| 指标 | 标准 |
|---|---|
| 迁移 | result ok，cross_node true |
| 用户请求 | GET/POST 0 失败 |
| route switch | service error 0 |
| dirty rate | raw 证据中记录具体 rate |
| tail latency | 每档输出 max / p95，不能只报平均值 |
| pathological | 如果不收敛，应明确失败模式和 rollback 行为 |

## 11. Cleanup isolation 场景

### 11.1 业务含义

迁移成功后，系统清理 source 或执行 suspend/delete/checkpoint，不应影响已经切到 target 的用户请求。

### 11.2 执行方式

用户窗口测试必须拆成两段：

1. `run-hot-migration --skip-final-actor-cleanup`，只覆盖用户迁移窗口和 post-switch observation。
2. route 已确认在 target 后，继续压测 60-300s，同时触发 cleanup。

关键参数：

```bash
--cleanup-isolation-duration 60s
--skip-final-actor-cleanup
```

### 11.3 验收标准

| 指标 | 标准 |
|---|---|
| cleanup window probe | 请求失败 0 |
| `service_errors_during_cleanup` | 0 |
| route target | 不回 source，不 target mismatch |
| target actor | 保持 RUNNING/ACTIVE |
| cleanup 失败 | 不能影响 target 服务；失败资源必须可审计、可清理 |

如果 final `SuspendActor/DeleteActor` 作用到同一个 target actor 并中止服务，该样本必须判定 cleanup isolation 失败，不能算用户窗口失败。

## 12. Checkpoint / Restore 路线复现

该路线不是当前热迁移主线，但用于验证 30s 跨节点恢复能力。

### 12.1 核心拆分

| 阶段 | 需要测量 |
|---|---|
| checkpoint RPC | pause/checkpoint 实际 RPC 时间 |
| runsc checkpoint | atelet/ateom 日志中的 runsc checkpoint 时间 |
| snapshot 文件 | `pages.img` logical size 和 actual disk usage |
| 跨节点复制 | 单流、2/3/4/5/8 stream copy 耗时 |
| restore RPC | resume/restore 实际 RPC 时间 |
| end-to-end | 从发起恢复到 actor RUNNING |

### 12.2 必须记录的文件大小

```bash
du -h <snapshot-dir>/*
ls -lh <snapshot-dir>/*
```

必须单独记录：

| 文件 | 说明 |
|---|---|
| `pages.img` | 主要数据体，16GiB 场景约 17.2GB |
| `checkpoint.img` | 元数据，通常很小 |
| `pages_meta.img` | 页面元数据 |
| `manifest.json` | snapshot manifest |

### 12.3 通过标准

| 目标 | 标准 |
|---|---|
| 10s checkpoint/restore | 当前数据不支持，不能作为通过标准 |
| 30s checkpoint/restore | checkpoint + copy + restore 核心路径小于 30s |
| restore body | 应为 sub-second 级，否则需要单独定位 |
| 瓶颈归因 | 必须拆出 runsc checkpoint、跨节点 copy、restore |

## 13. 生产级真实场景矩阵

基础 L7 smoke 不能替代以下场景。专业测试人员复现生产级验收时，必须按下表补齐。

| 优先级 | 场景 | 方法 | 必须证据 | 当前工具状态 |
|---|---|---|---|---|
| P0 | HTTP/POST/SSE/WebSocket 用户窗口 | 第 6 节命令 | 请求成功率、gap、route proof | 已具备 |
| P0 | 强一致 mutation | 第 8 节 | idempotency key、counter、state_version、last_mutation_id | 已具备 |
| P0 | Dirty memory | 第 10 节 | dirty rate、precopy 收敛、tail latency | 已具备单样本，需矩阵 |
| P0 | cleanup isolation | 第 11 节 | cleanup window 0 service error | 需继续补 |
| P1 | WebSocket terminal 多样本 | 第 9 节 | disconnect/missing/duplicate/sequence gap | 已具备 |
| P1 | 1GiB upload | 分块上传，记录 byte offset 和 checksum | final sha256、duplicate/missing range | 当前工具不足，需增强 workload/runner |
| P1 | 1GiB download | 完整读取或 Range resume | byte ranges、final sha256、EOF/RST | 当前工具不足，需增强 runner |
| P1 | Notebook/kernel | 运行 cell 时迁移 | kernel id、cell id、变量 checksum | 需新增 workload |
| P1 | Agent tool exactly-once | tool running 时迁移 | execution_count、result checksum | 需新增 workload |
| P2 | 多 actor 并发 | N=2/5/10 同时迁移 | 每 actor 成功率、P95/P99、no-free-target | 需编排脚本 |
| P2 | 故障注入/回滚 | target/source/route/readyz 分阶段注入 | blackhole/dual active/rollback proof | 需故障注入能力 |

## 14. 结果判定规则

### 14.1 可以写通过

只有满足以下条件，才可以写“该 cell 通过”：

| 条件 | 要求 |
|---|---|
| 进程退出码 | 0 |
| `hot_migration_summary.result` | `ok` |
| 跨节点 | true |
| HTTP | 请求数等于成功数 |
| POST | 请求数等于成功数 |
| route switch | 有 switched proof，service error 0 |
| SSE | summary ok，gap 可量化 |
| WebSocket | 若启用，summary ok，断连/缺失/重复/gap 为 0 |
| cleanup | 单独统计，不混入用户窗口 |

### 14.2 不能扩大结论

| 已测内容 | 不能扩大成 |
|---|---|
| 64KiB upload | 1GiB 上传无损 |
| 1MiB download | 1GiB 下载无损 |
| 简单 `/increment` | 外部 tool exactly-once |
| 单 actor | 多 actor 并发可靠 |
| 0/128/512/1024MiB/s 单样本 | 完整 dirty memory 矩阵 |
| 用户窗口通过 | cleanup isolation 通过 |
| 当前探针 0 失败 | 数学意义绝对 0ms downtime |

## 15. 结果汇总模板

每轮测试结束后，测试人员必须输出以下汇总表。

### 15.1 单 cell 汇总

| 字段 | 值 |
|---|---|
| run id |  |
| backend |  |
| scenario |  |
| actor template |  |
| source node |  |
| target node |  |
| cross-node |  |
| total_to_target_success_ms |  |
| commit_live_migration_ms |  |
| HTTP success/request |  |
| POST success/request |  |
| route-switched HTTP success/request |  |
| route-switched POST success/request |  |
| service errors |  |
| SSE max gap |  |
| WebSocket disconnect/missing/duplicate/gap |  |
| cleanup result |  |
| verdict | pass/fail |

### 15.2 矩阵汇总

| backend | scenario | verdict | total_to_target_success_ms | commit_live_migration_ms | HTTP | POST | max SSE gap | max WS gap | cleanup |
|---|---|---|---:|---:|---|---|---:|---:|---|
| Lustre | short |  |  |  |  |  |  |  |  |
| Lustre | slow |  |  |  |  |  |  |  |  |
| Lustre | upload |  |  |  |  |  |  |  |  |
| Lustre | download |  |  |  |  |  |  |  |  |
| Lustre | mixed |  |  |  |  |  |  |  |  |
| JuiceFS | short |  |  |  |  |  |  |  |  |
| JuiceFS | slow |  |  |  |  |  |  |  |  |
| JuiceFS | upload |  |  |  |  |  |  |  |  |
| JuiceFS | download |  |  |  |  |  |  |  |  |
| JuiceFS | mixed |  |  |  |  |  |  |  |  |
| JindoFS | short |  |  |  |  |  |  |  |  |
| JindoFS | slow |  |  |  |  |  |  |  |  |
| JindoFS | upload |  |  |  |  |  |  |  |  |
| JindoFS | download |  |  |  |  |  |  |  |  |
| JindoFS | mixed |  |  |  |  |  |  |  |  |

## 16. 清理规则

测试环境可能与其他业务共用，清理必须只作用于本轮测试资源。

允许清理：

| 资源 | 条件 |
|---|---|
| atespace | 名称必须匹配本轮 run id |
| actor | actor 的 atespace 必须匹配本轮 run id |
| worker pod | 只有 assignment 指向本轮 atespace 时允许删除 |
| snapshot 目录 | 路径必须是本轮测试专属路径 |
| 本地 checkpoint-state | 目录名必须能对应本轮测试 actor |

禁止清理：

| 资源 | 说明 |
|---|---|
| 非本轮 atespace | 可能属于其他业务或其他测试 |
| 非本轮 worker assignment | 不能用全局重启释放 |
| 整个 namespace | 除非明确是一次性测试 namespace |
| 整个存储 bucket/prefix | 必须只删本轮 prefix |

清理后必须记录：

```bash
go run ./cmd/kubectl-ate get workers -o json > "${OUT_DIR}/workers-after-cleanup.json"
go run ./cmd/kubectl-ate get actors -A -o json > "${OUT_DIR}/actors-after-cleanup.json"
kubectl get pods -A -o wide > "${OUT_DIR}/pods-after-cleanup.txt"
```

## 17. 归档规则

每轮复现测试必须保留：

| 文件 | 说明 |
|---|---|
| `command.sh` | 可复现命令 |
| `*.jsonl` | perfkit 原始事件 |
| `perfkit.log` | 命令输出 |
| `exit-code.txt` | 退出码 |
| `workers-before/after.json` | worker assignment 变化 |
| `actors-before/after.json` | actor 状态变化 |
| `pods-before/after.txt` | pod 落点 |
| `summary.json` | 汇总 |
| `SHA256SUMS.txt` | 证据校验 |

归档目录不得覆盖旧结果。每次测试必须使用新的 timestamp 目录。

## 18. 最小可复现顺序

专业测试人员第一次复现时，建议按这个顺序执行：

1. 第 4 节环境检查。
2. 第 6 节单 actor 用户窗口热迁移。
3. 第 7 节只跑 `BACKEND_FILTER=lustre SCENARIO_FILTER=short`。
4. 第 7 节跑完整 15 cell。
5. 第 8 节确认强一致 mutation proof。
6. 第 9 节 WebSocket terminal 重复 5 次。
7. 第 10 节 dirty memory 0/128/512/1024MiB/s 每档至少 5 次。
8. 第 11 节 cleanup isolation。
9. 第 12 节 checkpoint/restore 30s 路线。
10. 只在工具补齐后执行 1GiB upload/download、Notebook、tool exactly-once、多 actor、故障注入。

## 19. 当前已知边界

| 边界 | 说明 |
|---|---|
| 绝对 0ms downtime | 当前测试只能证明探针下 0 失败，不能证明数学意义 0ms |
| 大文件 | 当前 upload/download smoke 不是 1GiB 大文件验收 |
| cleanup | 用户窗口和 cleanup 必须拆开统计 |
| JindoFS | 用户窗口可用，但 FUSE/cleanup 可靠性需单独关注 |
| BeeGFS | 纯 IO 性能好，但开源版 client 限制和 RDMA 未验证通过 |
| Dirty memory | 单样本不能代表完整矩阵 |
| 多 actor | 单 actor 成功不能推断并发成功 |

## 20. 参考文件

| 用途 | 路径 |
|---|---|
| 可读结果报告 | `results/live-migration-evidence-20260724/live-migration-readable-report-20260726203950.zh.md` |
| 纯数据报告 | `results/live-migration-evidence-20260724/live-migration-data-report-20260726203617.zh.md` |
| 三后端矩阵脚本 | `results/live-migration-evidence-20260724/run-real-scenario-matrix.sh` |
| 最终用户窗口命令样例 | `results/live-migration-evidence-20260724/runs-supplemental-20260726043802/generic-livech-user-window-s43-skip-cleanup-20260726043802/command.sh` |
| Dirty memory 命令样例 | `results/live-migration-evidence-20260724/runs-supplemental-20260726034313/generic-livech-dirty1024-s40-20260726034313/command.sh` |
| 真实场景边界 | `results/live-migration-evidence-20260724/live-migration-real-scenario-canonical-plan-and-evidence-20260725231531.zh.md` |
| checkpoint/restore 性能归档 | `results/memory-performance-archive-20260721.md` |
