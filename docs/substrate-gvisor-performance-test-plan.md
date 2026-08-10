# Substrate + gVisor Scale and Performance Test Plan

## 背景

本文档用于规划 Substrate + gVisor 在 ACK 或其他 Kubernetes 集群上的规模与性能测试。目标不是只验证“能跑通”，而是系统性判断 Substrate 的 actor multiplexing、gVisor 沙箱、snapshot/restore、worker cache、对象存储和 Kubernetes 节点资源之间的综合性能边界。

本计划只定义测试目标、指标、场景、基线和结果格式，不包含实际执行动作。

性能测试不会一轮完成。本文档把测试拆成多轮递进：每一轮先回答一个有限问题，基于上一轮结果修正 workload、指标、SLO 和规模阶梯，再进入下一轮。任何一轮如果缺少可观测性、样本量或环境记录，结论只能作为 exploratory data，不能作为性能结论。

## 参考来源

测试项参考了主流沙箱和轻量虚拟化项目公开采用的性能维度：

| 来源 | 可借鉴的性能维度 |
|---|---|
| gVisor Performance Guide | memory access、memory usage、CPU、syscall、startup、network、filesystem |
| gVisor filesystem/user guide | overlay、tmpfs、filesystem passthrough、host filesystem 隔离与性能权衡 |
| Firecracker specification / performance testing | microVM boot time、VMM memory overhead、I/O throughput、network latency、density、snapshot/restore |
| Firecracker NSDI paper / AWS public material | serverless 场景下的快速启动、低内存开销、高密度隔离 |
| Kata Containers metrics/tests | boot time、memory footprint、storage I/O、network throughput、density、Kubernetes runtime 集成 |
| Cloud Hypervisor performance metrics | VM boot time、block I/O、network throughput、device model 影响 |
| container sandbox 对比研究 | runc、gVisor、Kata、microVM 在 syscall、filesystem、network、startup 上的差异 |
| Substrate benchmarking 目录 | Locust 负载模型、ateapi/actor workload、Prometheus 监控、自动化 benchmark runner |

外部链接清单：

- https://gvisor.dev/docs/architecture_guide/performance/
- https://gvisor.dev/docs/user_guide/filesystem/
- https://github.com/firecracker-microvm/firecracker/blob/main/SPECIFICATION.md
- https://firecracker-microvm.github.io/
- https://www.usenix.org/system/files/nsdi20-paper-agache.pdf
- https://github.com/kata-containers/kata-containers
- https://github.com/kata-containers/tests
- https://kata-containers.github.io/kata-containers/design/kata-2-0-metrics/
- https://github.com/cloud-hypervisor/cloud-hypervisor/blob/main/docs/performance_metrics.md
- https://www.usenix.org/system/files/hotcloud19-paper-young.pdf

## 测试目标

### 核心问题

1. Substrate + gVisor 的 actor 启动、暂停、恢复、提交在不同规模下的 P50/P95/P99 延迟是多少。
2. actor multiplexing 能否在保证恢复延迟的前提下显著提升单节点和集群承载密度。
3. gVisor 相比 runc、Kata、Firecracker 类隔离方案的主要性能损耗集中在哪里。
4. snapshot 本地命中、跨节点恢复、对象存储上传下载、worker locality 对端到端延迟的影响有多大。
5. ACK 上 KVM、OSS、ACR、网络、磁盘、节点规格对 Substrate + gVisor 的限制是什么。
6. 在突发流量、长时间 soak、故障注入场景下，控制面和 worker cache 是否保持稳定。

### 非目标

本计划不做安全强度评估，不比较漏洞面大小，不验证多租户逃逸能力。安全隔离只作为性能基线的上下文，不作为本轮测试结论。

## 被测系统和基线

### 必测基线

| 基线 | 目的 | 说明 |
|---|---|---|
| host process | 理论上限 | 在节点上直接运行 workload，得到 CPU、memory、filesystem、network 的本机上限 |
| runc/containerd | 容器性能基准 | Kubernetes 默认 runtime 或接近默认 runtime，作为低隔离开销基线 |
| gVisor runsc systrap | gVisor 默认常见路径 | 不依赖 KVM，观察 syscall、filesystem、network 成本 |
| gVisor runsc KVM | Substrate 目标沙箱路径 | 需要 `/dev/kvm`，对 ACK nested virtualization 关键 |
| Substrate + gVisor | 核心被测对象 | 包含 ateapi、atelet、ateom-gvisor、worker pool、snapshot 流程 |

### 建议扩展基线

| 基线 | 目的 | 是否进入最终报告 |
|---|---|---|
| Kata Containers | VM-based container runtime 对照 | 建议进入，尤其关注 boot/memory/density |
| Firecracker | microVM 对照 | 建议进入，尤其关注 boot/snapshot/density |
| Cloud Hypervisor | modern VMM 对照 | 如果 Substrate 后续支持 microVM backend，应进入 |
| Wasmtime/WasmEdge | WASM 沙箱对照 | 仅在 workload 可移植到 WASM 时进入 |

### 基线可比性规则

不同沙箱的隔离边界和启动语义不同，最终报告必须避免把不可比的数据放进同一个排名。基线拆成三类：

| 类别 | 可比较对象 | 可回答的问题 | 不应回答的问题 |
|---|---|---|---|
| native runtime microbench | host、runc、gVisor systrap、gVisor KVM、Kata RuntimeClass | CPU、syscall、filesystem、network、sandbox startup 的相对 overhead | Substrate actor lifecycle 性能 |
| Kubernetes RuntimeClass 对比 | runc RuntimeClass、gVisor RuntimeClass、Kata RuntimeClass | 同一 Pod workload 在 Kubernetes 下的 runtime 差异 | Firecracker 裸 microVM 与 Kubernetes Pod 的直接优劣 |
| Substrate lifecycle 对比 | Substrate + gVisor，不同 worker/snapshot/locality 配置 | actor create/pause/resume/commit、multiplexing、snapshot locality | 非 Substrate runtime 的 actor 语义排名 |

Firecracker、Cloud Hypervisor、libkrun 或其他 microVM 只有在具备同等 workload harness、同等网络路径、同等存储路径和同等启动语义时，才能进入主结论对比。否则它们只能作为行业参考基线，用于解释设计空间。

### 版本固定要求

每次测试必须记录：

- Substrate git commit。
- Kubernetes 版本。
- ACK 集群 ID、region、node image、node kernel。
- 节点实例规格、CPU 型号、NUMA、内存、磁盘类型、磁盘容量。
- containerd、runc、runsc、Kata、Firecracker、Cloud Hypervisor 版本。
- gVisor platform：`systrap` 或 `kvm`。
- gVisor filesystem 配置：overlay、tmpfs、lisafs、directfs、gofer 相关配置。
- CNI、Pod 网络模式、MTU。
- 对象存储 endpoint、bucket region、是否内网访问、是否开启校验和。
- 镜像仓库类型和 region。

### 测试准入门槛

正式规模测试前必须通过 Phase 0 gate。未通过时只能执行 smoke 或 exploratory run，不能发布性能结论。

| 准入项 | 最低要求 | 失败处理 |
|---|---|---|
| trace/correlation id | 每个 actor lifecycle request 有唯一 `run_id`、`actor_id`、`operation_id` | 停止 10 节点以上测试 |
| stage latency 覆盖率 | create/pause/resume/commit 的关键 stage 覆盖率 >= 99% | 补 instrumentation 后重跑 |
| 时钟同步 | 所有节点 NTP 正常，节点间时钟偏差 <= 10 ms | 修复节点时间后重跑 |
| metrics scrape | Prometheus scrape success rate >= 99% | 修复采集或降低规模 |
| log retention | ateapi、atelet、ateom、runsc、Kubernetes events 可按 run-id 检索 | 不进入正式 run |
| KVM readiness | gVisor KVM 场景中所有 worker 节点存在 `/dev/kvm` | 节点隔离或重建 |
| object storage probe | PUT/GET/HEAD/LIST smoke 成功，记录 P50/P95/P99 | 修复 OSS endpoint/credential |
| registry probe | 测试镜像 pull smoke 成功，记录 cold/warm pull latency | 修复 ACR/registry |
| result schema | runner 能输出 `lifecycle-events.jsonl` 和 percentile CSV | 修复 runner |

### worker pool 控制变量

每个 Substrate run 必须固定并记录 worker pool 配置，否则密度结果不可比较。

| 变量 | 必填值 |
|---|---|
| workers per node | 每节点 worker pod 数，或自动计算公式 |
| worker CPU request/limit | request、limit、是否 Guaranteed/Burstable |
| worker memory request/limit | request、limit、OOM policy |
| idle worker reserve | 预留 idle worker 比例，例如 10% 或固定数量 |
| max active actors per worker | 如果实现层没有硬限制，记录测试约束 |
| max paused actors per worker/node | local snapshot 和 metadata 的测试约束 |
| image warmup | worker image、workload image、runsc asset 是否预拉取 |
| local snapshot storage limit | 每节点本地 snapshot 可用空间和 GC policy |
| actor ratio | active、paused-local、paused-external、crashed 的目标比例 |
| anti-affinity/locality policy | same-worker、same-node、cross-node 的调度策略 |

推荐默认控制组：

| Profile | 用途 | workers/node | active ratio | paused-local ratio | paused-external ratio | idle reserve |
|---|---|---:|---:|---:|---:|---:|
| `smoke` | Day-1 验证 | 4 | 20% | 60% | 20% | 1 worker/node |
| `baseline` | 10 节点基线 | 8 | 10% | 70% | 20% | 10% |
| `density` | 密度边界 | 递增：8/16/32 | 5%/10%/20% | 60% | 20% | 5% |
| `burst` | 突发恢复 | 8/16 | 0% 到 50% ramp | 70% 起始 | 30% 起始 | 20% |
| `soak` | 长稳 | 8 | 10% | 70% | 20% | 10% |

## 指标矩阵

### 1. 生命周期延迟

| 指标 | 口径 | 维度 |
|---|---|---|
| actor create latency | API request 到 actor Ready | P50/P90/P95/P99/max |
| actor resume latency | ResumeActor 到第一个业务请求成功 | P50/P90/P95/P99/max |
| actor pause latency | PauseActor 到 worker 释放或 snapshot 完成 | P50/P90/P95/P99/max |
| actor commit latency | Commit 到 snapshot 持久化完成 | P50/P90/P95/P99/max |
| runsc create latency | ateom 调用 runsc create 的耗时 | per node、per workload |
| runsc restore latency | runsc restore 到 workload 可服务 | local/external snapshot |
| cold start latency | 镜像未缓存、runsc 未缓存、无 local snapshot | per image size |
| warm start latency | 镜像、sandbox binary、snapshot 本地命中 | per actor template |

必须拆分端到端耗时：

- client 到 ateapi。
- ateapi 调度和状态写入。
- worker 选择和 locality 判断。
- atelet RPC。
- snapshot manifest 读写。
- object storage upload/download。
- runsc checkpoint/restore。
- workload readiness probe 或业务探活。

### 2. CPU 和 syscall

| 指标 | 工具 | 说明 |
|---|---|---|
| CPU throughput | sysbench、stress-ng、业务 workload | 单 actor 与多 actor 对比 |
| syscall latency | lmbench、perf bench、custom Go syscall bench | open/stat/read/write/futex/epoll/socket |
| context switch rate | perf、pidstat、eBPF | 观察 gVisor sentry/gofer 成本 |
| CPU steal/irq/softirq | node exporter、mpstat | 网络和存储压力下必须记录 |
| per-component CPU | Prometheus、cAdvisor | ateapi、atelet、ateom、worker、Valkey |

建议 syscall 子项：

- `getpid`
- `clock_gettime`
- `futex`
- `epoll_wait`
- `openat`
- `statx`
- `read`
- `write`
- `sendmsg`
- `recvmsg`
- `connect`
- `accept`
- `mmap`
- `fork/exec`

### 3. 内存

| 指标 | 口径 |
|---|---|
| actor RSS/PSS | workload 进程内与节点视角分别记录 |
| gVisor sentry/gofer memory | per sandbox |
| worker pod memory | idle、active、snapshotting 三态 |
| control plane memory | ateapi、atelet、Valkey、registry 相关组件 |
| snapshot memory image size | uncompressed、compressed、uploaded object size |
| node allocatable 消耗 | pod request/limit 与实际 usage |
| density memory overhead | 每增加 100/1000 actor 的边际内存成本 |

必须区分：

- active actor。
- paused actor with local snapshot。
- paused actor with external snapshot only。
- idle worker。
- busy worker。
- failed/crashed actor。

### 3.1 内存尺寸梯度测试

目的：回答“不同 actor 内存尺寸下，checkpoint、restore、跨节点 recover、资源消耗和规模衰减如何变化”。这个测试必须把 actor 内存尺寸作为一等测试维度，避免用 `counter` 这类极小状态 workload 的结果代表真实 agent。

#### 内存梯度

正式测试至少包含以下内存档位。每个档位都要记录目标内存、实际触达内存、快照大小和恢复耗时。

| bucket | 目标内存 | 用途 | 最低样本量 | 进入规模测试条件 |
|---|---:|---|---:|---|
| `mem-64m` | 64 MiB | 小状态 baseline，对齐当前 counter 类结果 | 1000 restore | smoke 必测 |
| `mem-256m` | 256 MiB | 轻量 agent、少量上下文和缓存 | 1000 restore | R1/R2 必测 |
| `mem-512m` | 512 MiB | 中等工具型 agent | 1000 restore | R1/R2 必测 |
| `mem-1g` | 1 GiB | 中等长上下文或 workspace cache | 1000 restore | R1/R2 必测 |
| `mem-2g` | 2 GiB | 大状态 agent 起点 | 500 restore | R2 必测，R4 视资源 |
| `mem-4g` | 4 GiB | 大内存 checkpoint/restore 拐点测试 | 300 restore | R2 必测，R4 抽样 |
| `mem-8g` | 8 GiB | 压力档，验证对象存储和 restore 带宽 | 100 restore | 仅在 4 GiB 稳定后进入 |
| `mem-16g` | 16 GiB | 边界档，寻找不可接受拐点 | 50 restore | 可选，不进入常规回归 |

如果节点规格无法承载高档位，允许把 `mem-8g` 和 `mem-16g` 标记为 skipped，但必须在报告中说明跳过原因、节点可用内存、worker memory limit 和成本预算。

#### workload 要求

内存 workload 不能只 `malloc` 后不访问。必须保证页面真实进入 RSS，并区分可压缩和不可压缩内存。

| workload profile | 内存模式 | 目的 |
|---|---|---|
| `memory-zeroed` | 分配后写入重复模式 | 测试快照压缩上限，不代表真实 workload |
| `memory-random` | 分配后写入伪随机数据，记录 seed | 测试不可压缩内存的最差路径 |
| `memory-dirty-10` | restore 后只修改 10% 页面再 checkpoint | 测试增量 dirty set 效率 |
| `memory-dirty-50` | restore 后修改 50% 页面再 checkpoint | 测试中等变化状态 |
| `memory-dirty-100` | 每轮全量改写 | 测试最差 checkpoint 写入压力 |
| `memory-agent-cache` | heap + mmap + 文件 cache 混合 | 模拟真实 agent 上下文、索引、依赖缓存 |

每个进程必须暴露 readiness 和 memory touch 完成信号。只有确认实际 RSS 达到目标内存的 95% 以上，才能开始 checkpoint/restore 采样；否则该样本必须标记为 invalid。

#### 每个内存档位的测试步骤

1. 启动 actor，等待 workload 完成内存分配和页面触达。
2. 记录 actor RSS/PSS、worker pod memory、node memory、cgroup memory。
3. 执行 pause/checkpoint，记录 checkpoint 总耗时和 stage breakdown。
4. 记录 snapshot 文件列表、逻辑大小、实际 populated bytes、压缩后对象大小和对象数。
5. 执行 warm local resume，记录恢复到业务探活成功的 P50/P95/P99。
6. 执行 same-node different-worker restore，区分 worker 复用收益。
7. 执行 cross-node external restore，记录下载、解压、runsc restore 和业务探活耗时。
8. 执行 dirty workload 后再次 checkpoint，记录 dirty ratio 对快照大小和 checkpoint latency 的影响。
9. 每档重复至少 3 轮；用于 P99 结论的档位至少 1000 个有效样本。

#### 效率指标

内存梯度测试不能只报告毫秒数，还必须报告效率和衰减。

| 指标 | 计算方式 | 用途 |
|---|---|---|
| checkpoint throughput | `snapshot_populated_bytes / checkpoint_duration_sec` | 判断写快照效率 |
| restore throughput | `snapshot_populated_bytes / restore_duration_sec` | 判断恢复效率 |
| compression ratio | `snapshot_compressed_bytes / snapshot_populated_bytes` | 判断数据可压缩性 |
| local restore advantage | `external_restore_p99 / local_restore_p99` | 判断本地命中收益 |
| cross-node penalty | `cross_node_restore_p99 / same_node_restore_p99` | 判断跨节点恢复损耗 |
| memory overhead ratio | `node_memory_delta / actor_rss_bytes` | 判断 gVisor/worker 额外内存 |
| p99 growth per GiB | `delta_p99_ms / delta_memory_gib` | 判断延迟随内存增长的斜率 |
| scale degradation | `p99_100_nodes / p99_10_nodes` at same memory bucket | 判断规模化衰减 |

#### 节点规模组合

内存档位和节点规模必须交叉测试，但不能一次性把所有组合打满。推荐递进：

| 阶段 | 节点 | 内存档位 | actor 数 | 目的 |
|---|---:|---|---:|---|
| calibration | 1 | 64MiB/256MiB/1GiB/4GiB | 10 到 100 | 确认 workload 真实占用内存和快照字段完整 |
| baseline | 10 | 64MiB/256MiB/512MiB/1GiB/2GiB/4GiB | 100 到 1000 | 建立 10 节点内存效率曲线 |
| pressure | 10 | 4GiB/8GiB/16GiB | 10 到 200 | 找到单集群对象存储和节点内存拐点 |
| scale-sample | 50 | 256MiB/1GiB/4GiB | 1000 到 10000 | 判断规模化衰减 |
| scale-sample | 100 | 256MiB/1GiB/4GiB | 5000 到 20000 | 判断控制面和 OSS 是否放大 |

高内存档位的 actor 数必须受节点可用内存、worker memory limit、对象存储成本和恢复并发限制控制。报告必须同时给出“同内存档位不同节点规模”和“同节点规模不同内存档位”的趋势。

### 4. 文件系统和存储

| 指标 | 工具 | 场景 |
|---|---|---|
| sequential read/write | fio | durable dir、本地盘、emptyDir、overlay |
| random read/write | fio | agent workspace、package cache、small file |
| fs metadata ops | mdtest、fs_mark、custom bench | 大量小文件、git checkout、node_modules |
| snapshot write bandwidth | atelet metrics、object storage logs | checkpoint |
| snapshot read bandwidth | atelet metrics、object storage logs | restore |
| object storage latency | awscli/s5cmd/custom probe | PUT/GET/HEAD/LIST |
| local snapshot copy latency | atelet timing | same-node resume |

必须覆盖 workload：

- 单大文件读写。
- 多小文件创建/删除。
- git clone/checkout。
- Python/Node dependency install cache。
- SQLite 或轻量 KV 写入。
- durable dir 参与 snapshot。

### 5. 网络

| 指标 | 工具 | 说明 |
|---|---|---|
| TCP throughput | iperf3 | pod-to-pod、pod-to-service、pod-to-external |
| TCP latency | sockperf、netperf | P50/P99 |
| HTTP QPS/latency | wrk、hey、Locust | actor 服务入口 |
| gRPC latency | ghz、Locust gRPC client | ateapi、atelet |
| connection churn | custom bench | 短连接高频 connect/close |
| packet drops/retransmits | node exporter、ss、ethtool | 高并发场景 |
| DNS latency | CoreDNS metrics、custom probe | actor 冷启动和外部依赖 |

网络拓扑必须至少包括：

- same node pod-to-pod。
- cross node pod-to-pod。
- client to atenet/router to actor。
- actor to external internet。
- actor to object storage endpoint。
- actor to registry endpoint。

网络结果必须分成三类报告：

| 网络类别 | 链路 | 主要用途 |
|---|---|---|
| actor 数据面 | client -> atenet/router -> actor，actor -> external service | 判断业务请求延迟、连接 churn、streaming response |
| Substrate 控制面 | benchmark runner -> ateapi，ateapi -> atelet，atelet -> ateom | 判断生命周期 RPC 和调度链路延迟 |
| 云服务依赖 | actor/atelet -> OSS，node -> ACR/registry，node -> Kubernetes API | 判断 cold start、snapshot 和云服务限流 |

三类网络不能混合成一个 P99。控制面抖动、数据面拥塞和云服务限流必须单独归因。

### 6. snapshot/checkpoint/restore

| 指标 | 口径 |
|---|---|
| checkpoint total latency | API 到 snapshot manifest/object 完整可用 |
| restore total latency | API 到 actor 可服务 |
| runsc checkpoint latency | ateom 内部阶段 |
| runsc restore latency | ateom 内部阶段 |
| snapshot manifest latency | read/write/list |
| snapshot object count | per snapshot |
| snapshot bytes | memory、filesystem、manifest、metadata |
| local snapshot hit rate | same-node resume 命中比例 |
| external snapshot fallback rate | local miss 后对象存储恢复比例 |
| failed snapshot rate | timeout、upload failed、restore failed |
| snapshot GC impact | GC 期间 actor latency 和 node I/O |

关键对比：

- local snapshot restore vs external snapshot restore。
- memory-only snapshot vs memory + durable dir snapshot。
- same worker resume vs same node different worker vs cross-node restore。
- object storage 内网 endpoint vs 公网 endpoint。
- snapshot size 从 64 MiB 到 16 GiB 的曲线。

### 7. 密度和规模

| 指标 | 口径 |
|---|---|
| max active actors per node | 满足 latency SLO 前的最大值 |
| max paused actors per node | local snapshot 和 metadata 不失控的最大值 |
| max total actors per cluster | active + paused |
| worker utilization | busy/idle/snapshotting/crashed |
| multiplexing ratio | actors/workers |
| scheduler rejection rate | no free workers、locality conflict、quota |
| control-plane QPS | ateapi requests/sec |
| Valkey ops/sec and latency | GET/SET/list/watch |
| Kubernetes API QPS | pod、secret、event、lease、CRD |

规模阶梯建议：

- 单节点：10、50、100、250、500、1000 actor。
- 10 节点：100、500、1000、2500、5000、10000 actor。
- 50 节点：10000、25000、50000 actor。
- 100 节点：50000、100000+ actor。

每个阶梯至少保持 30 分钟稳定窗口；soak 场景保持 12 到 24 小时。

### 8. 稳定性和故障恢复

| 场景 | 指标 |
|---|---|
| ateapi restart | in-flight API error rate、恢复时间 |
| atelet restart | worker 状态重建时间、orphan worker 数 |
| Valkey restart | actor 状态一致性、恢复时间、错误率 |
| node drain | actor 迁移/恢复延迟、失败率 |
| object storage 5xx/timeout | snapshot fallback、actor crash rate |
| registry pull slow/fail | cold start 延迟、失败率 |
| network partition | split-brain、重复 worker assignment |
| local disk pressure | snapshot GC、checkpoint failure |
| KVM device missing | worker admission、错误语义 |

### 故障注入控制

故障注入必须限制 blast radius，避免把测试集群打到无法归因。

| 故障 | 注入方式 | 范围 | 持续时间 | 通过条件 | 停止条件 |
|---|---|---|---|---|---|
| ateapi restart | 删除 1 个 ateapi Pod 或滚动重启 Deployment | 1 replica | 1 次 | in-flight error 可恢复，状态无丢失 | API error > 5% 持续 5 分钟 |
| atelet restart | 删除 1 个节点上的 atelet Pod | 1 node | 1 次 | worker 状态可重建，无 orphan worker 增长 | 该节点 actor 失败率 > 10% |
| Valkey restart | 删除 primary 或执行受控 restart | 1 次 | 1 次 | actor 状态恢复，错误率回落 | 状态不一致或恢复超过 10 分钟 |
| node drain | drain 1 个 worker 节点 | <= 10% 节点 | 1 次 | actor 可迁移或失败语义明确 | NotReady 节点 > 10% |
| OSS timeout | 对 OSS endpoint 注入 1s/5s latency 或 5xx | 10% 请求或 1 个节点 | 5 min | fallback 和 error reason 清晰 | snapshot failure > 5% |
| registry pull slow | 限制 registry 带宽或清空少量节点 image cache | 1-2 nodes | 10 min | cold start 延迟可解释 | pull failure > 5% |
| local disk pressure | 写入 filler 到 snapshot 盘 | 1 node | 到 85% usage | GC 或 backpressure 生效 | disk usage > 90% |
| KVM missing | 从 1 个测试节点移除/隔离 KVM readiness | 1 node | 1 run | worker admission 拒绝且错误明确 | 调度到无 KVM 节点 |

## 实验矩阵

测试执行必须从固定矩阵生成 run，不能临时手工组合。每个 run 至少包含以下字段：

| 字段 | 含义 |
|---|---|
| `run_id` | 唯一 ID，建议格式 `<round>-<phase>-<baseline>-<nodes>n-<actors>a-<workload>-<timestamp>` |
| `round` | 第几轮测试，例如 R0/R1/R2/R3/R4/R5 |
| `phase` | `smoke`、`runtime`、`lifecycle`、`scale`、`fault`、`soak`、`regression` |
| `baseline` | host、runc、gvisor-systrap、gvisor-kvm、substrate-gvisor、kata、firecracker-ref |
| `node_count` | 节点数量 |
| `workers_per_node` | 每节点 worker 数 |
| `worker_profile` | `smoke`、`baseline`、`density`、`burst`、`soak` |
| `actor_count` | actor 总数 |
| `active_ratio` | active actor 比例 |
| `paused_local_ratio` | 本地 snapshot paused actor 比例 |
| `paused_external_ratio` | 仅 external snapshot paused actor 比例 |
| `workload` | `idle-heavy`、`tool-heavy`、`memory-heavy`、`code-heavy`、`chat-serving` 或 microbench 名称 |
| `memory_profile` | `none`、`memory-zeroed`、`memory-random`、`memory-dirty-10`、`memory-dirty-50`、`memory-dirty-100`、`memory-agent-cache` |
| `target_memory_bytes` | actor workload 目标触达内存 |
| `validated_rss_bytes` | checkpoint 前 workload 实际 RSS，必须来自 cgroup/procfs/metrics |
| `dirty_memory_ratio` | 本轮 checkpoint 前被修改的页面比例 |
| `snapshot_policy` | `none`、`memory-only`、`memory+durable-dir`、`external-only`、`local-preferred` |
| `snapshot_size_bucket` | `none`、`64MiB`、`256MiB`、`512MiB`、`1GiB`、`2GiB`、`4GiB`、`8GiB`、`16GiB` |
| `duration` | 稳定窗口或总运行时长 |
| `warmup` | 预热时长或预热操作数 |
| `cooldown` | 冷却时长 |
| `repeats` | 重复次数 |
| `expected_output` | 必须生成的 raw data、metrics、logs、profiles |
| `abort_condition` | 立即停止条件 |

### 推荐 run matrix

| run set | round | baseline | nodes | workers/node | actors | workload | snapshot | duration | repeats | 目的 |
|---|---|---|---:|---:|---:|---|---|---|---:|---|
| `R0-smoke-runtime` | R0 | runc、gvisor-kvm | 1 | N/A | N/A | cpu/syscall/fio/iperf3 | none | 10 min | 3 | 验证工具链和 runtime 差异方向 |
| `R0-smoke-lifecycle` | R0 | substrate-gvisor | 1 | 4 | 100 | idle-heavy | local-preferred | 20 min | 3 | 验证生命周期和 schema |
| `R1-runtime-baseline` | R1 | host、runc、gvisor-systrap、gvisor-kvm、kata | 1 | N/A | N/A | cpu/syscall/fs/net/startup | none | 30 min | 5 | 建立单节点 runtime 基线 |
| `R1-substrate-1n` | R1 | substrate-gvisor | 1 | 4/8/16 | 100/500/1000 | idle-heavy、tool-heavy | memory-only、memory+durable-dir | 30 min | 5 | 建立单节点 actor lifecycle 基线 |
| `R2-ack-10n` | R2 | substrate-gvisor | 10 | 8 | 100/500/1000/2500/5000/10000 | idle-heavy、tool-heavy、code-heavy | local-preferred、external-only | 30 min/step | 5 | 找到 10 节点密度曲线 |
| `R2-memory-gradient` | R2 | substrate-gvisor | 10 | 8 | 100/500/1000 | memory-zeroed、memory-random、memory-dirty-10、memory-dirty-50、memory-dirty-100 | 64MiB/256MiB/512MiB/1GiB/2GiB/4GiB/8GiB/16GiB | 30 min/step | 3-5 | 建立内存尺寸到 checkpoint/restore 效率曲线 |
| `R2-snapshot-size` | R2 | substrate-gvisor | 10 | 8 | 1000 | memory-heavy | 64MiB/256MiB/1GiB/4GiB/16GiB | 30 min/step | 5 | 建立 snapshot size 到 restore latency 曲线 |
| `R3-burst` | R3 | substrate-gvisor | 10/50 | 8/16 | 1000/5000/10000 | idle-heavy、chat-serving | local-preferred | 60 min | 5 | 验证突发恢复和控制面饱和点 |
| `R3-fault` | R3 | substrate-gvisor | 10 | 8 | 5000 | mixed-agent | local-preferred | 60 min | 3 | 验证单点故障和云依赖抖动 |
| `R4-memory-scale` | R4 | substrate-gvisor | 10/50/100 | 8/16 | 1000/5000/10000/20000 | memory-random、memory-agent-cache | 256MiB/1GiB/4GiB | 30 min/step | 3 | 验证内存尺寸在不同节点规模下的衰减 |
| `R4-scale` | R4 | substrate-gvisor | 10/50/100 | 8/16/32 | 10000/25000/50000/100000 | mixed-agent | local-preferred | 30 min/step | 3 | 验证扩展边界和节点规模趋势 |
| `R4-soak` | R4 | substrate-gvisor | 50 | 8 | 50000 | mixed-agent | local-preferred | 12-24 h | 1-3 | 验证长稳和资源泄漏 |
| `R5-regression` | R5 | runc、gvisor-kvm、substrate-gvisor | 1/10 | 4/8 | 100/1000/5000 | idle-heavy、tool-heavy | local-preferred | 20 min/step | 3 | 固化 nightly/weekly 回归 |

`mixed-agent` 表示按照固定比例混合：`idle-heavy 50%`、`tool-heavy 20%`、`code-heavy 20%`、`chat-serving 10%`。如果加入 `memory-heavy`，必须单独标记 snapshot size bucket，避免污染总体 P99。

### 中止条件

任何正式 run 满足以下条件时应立即停止、保留现场并标记为 aborted：

- API error rate 连续 5 分钟 > 5%。
- 节点 OOM、NotReady 或 KVM 缺失影响 > 10% 测试节点。
- Valkey 或 Kubernetes API 不可用超过 2 分钟。
- object storage PUT/GET error rate 连续 5 分钟 > 5%。
- local disk 使用率 > 90% 且 snapshot GC 无法回收。
- 测试成本超过本轮预算上限。
- metrics scrape success rate 低于 95%，导致结果不可归因。

## 测试场景

### 场景 A：runtime microbenchmark

目的：隔离 gVisor 本身的性能成本。

基线：

- host process。
- runc/containerd。
- gVisor systrap。
- gVisor KVM。
- Kata。
- Firecracker。

workload：

- CPU：sysbench CPU、stress-ng matrix。
- syscall：lmbench 或 custom Go bench。
- filesystem：fio、fs_mark、git checkout。
- network：iperf3、sockperf、wrk。
- startup：创建 1/10/100 个 sandbox。

输出：

- 各 runtime 相对 runc 的 overhead。
- gVisor systrap vs KVM 的差异。
- filesystem/network/syscall 中最差的三项。

### 场景 B：Substrate actor lifecycle

目的：评估 Substrate 控制面和 actor 生命周期。

步骤：

1. 部署固定 worker pool。
2. 创建 N 个 actor template。
3. 对每个 actor 执行 create、request、pause、resume、commit、delete。
4. 对比 cold/warm/local/external snapshot。
5. 每个规模阶梯重复至少 5 轮。

指标：

- create/resume/pause/commit P50/P95/P99。
- worker assignment latency。
- snapshot latency breakdown。
- API error rate。
- worker utilization。

### 场景 C：snapshot locality

目的：验证 Substrate 的核心优化是否来自本地 snapshot 命中。

变量：

- local snapshot enabled/disabled。
- external snapshot only。
- same-node scheduling preference enabled/disabled。
- node count：1、10、50、100。
- actor memory：64 MiB、256 MiB、1 GiB、4 GiB、16 GiB。
- durable dir：0、100 MiB、1 GiB、10 GiB。

需要记录：

- local hit rate。
- local miss reason。
- cross-node restore latency。
- object storage bytes and requests。
- locality conflict 导致的调度失败或排队。

### 场景 D：agent-like workload

目的：模拟真实 agent 特征，而不是只跑 synthetic benchmark。

workload 组合：

- idle-heavy：90% 时间等待，10% 时间 CPU/IO burst。
- tool-heavy：频繁 fork/exec、读写小文件、HTTP 调用。
- memory-heavy：长上下文、embedding cache、模型客户端缓存。
- code-heavy：git checkout、npm/pip install、pytest。
- chat-serving：短请求、长连接、streaming response。

actor 生命周期：

- create。
- idle 5 到 30 分钟。
- resume 后处理 1 到 20 个请求。
- pause。
- 随机 commit。
- 随机删除。

目标：

- 判断 multiplexing ratio 到多少时 P99 resume 仍可接受。
- 判断 agent workspace 对 snapshot size 的影响。
- 判断 fork/exec 和小文件 workload 是否成为 gVisor 瓶颈。

### 场景 E：突发高并发

目的：验证 burst 下控制面和 worker pool 是否稳定。

负载模型：

- 0 到 1000 actor resume in 60 秒。
- 0 到 10000 actor resume in 5 分钟。
- 10 倍瞬时 QPS spike。
- 同时 20% actor pause，20% actor resume，5% actor commit。

指标：

- API saturation point。
- queue time。
- no free workers count。
- failed assignment reason。
- Valkey latency。
- worker cache staleness。
- Kubernetes API throttling。

### 场景 F：长时间 soak

目的：暴露内存泄漏、缓存膨胀、snapshot 垃圾、状态漂移。

配置：

- 10 节点起步，建议 50 节点。
- 5000 到 50000 actor。
- 12 到 24 小时。
- 每分钟随机 create/pause/resume/commit/delete。
- 每小时做一次局部故障注入。

必须观察：

- ateapi/atelet RSS 曲线。
- Valkey memory 和 key count。
- snapshot object count。
- local disk usage。
- worker cache 与真实 worker 状态差异。
- P99/P999 latency 漂移。

### 场景 G：ACK 专项

目的：验证 ACK 上的 provider-specific 限制。

变量：

- ECS 规格：c9i、g8i、其他支持 nested virtualization 的实例。
- KVM module pre-load vs DaemonSet loader。
- OSS 内网 endpoint vs 公网 endpoint。
- ACR 同 region vs 跨 region。
- Terway/Flannel 或 ACK 当前网络插件配置。
- ContainerOS kernel 版本。

检查项：

- `/dev/kvm` 可用率。
- runsc KVM 成功率。
- OSS snapshot throughput。
- ACR image pull latency。
- node reboot 后 worker readiness。
- cluster autoscaler 或 nodepool 扩缩容后的恢复时间。

### ACK 配额和云资源前置检查

ACK 专项测试前必须记录云资源配额，否则规模测试失败无法区分是 Substrate 瓶颈还是云侧限制。

| 资源 | 检查项 | 影响 |
|---|---|---|
| ECS | 实例规格库存、按量配额、vCPU 配额、系统盘/数据盘 IOPS | 节点扩容、磁盘 I/O、snapshot 本地读写 |
| ENI/IP | Pod IP 配额、ENI 配额、交换机可用 IP | Pod density、worker pool 扩展 |
| ACK | 单节点 Pod 上限、API Server QPS/限流、托管组件版本 | worker 数、CRD watch、控制面延迟 |
| OSS | bucket region、内网 endpoint、PUT/GET/LIST QPS、带宽、请求费用 | checkpoint/restore 延迟和成本 |
| ACR/registry | 同 region、pull QPS、镜像大小、节点镜像缓存 | cold start 和扩容恢复 |
| NAT/EIP/SLB | 出公网带宽、连接数、跨区流量 | actor external API、registry/OSS 公网路径 |
| 日志/监控 | Prometheus 存储、日志采集吞吐、retention | P99 抖动、成本、结果完整性 |

每轮 ACK 规模测试都要在 `environment.yaml` 中记录 quota snapshot，并在最终报告中标记是否触达云侧配额。

## 工具和采集

### Benchmark 工具

| 类别 | 工具 |
|---|---|
| Substrate load | `benchmarking/locust`、Locust、custom gRPC client |
| CPU | sysbench、stress-ng |
| syscall | lmbench、perf bench、custom Go benchmark |
| filesystem | fio、fs_mark、mdtest、git benchmark |
| network | iperf3、sockperf、netperf、wrk、hey、ghz |
| object storage | awscli、s5cmd、custom S3 probe |
| profiling | perf、bcc/eBPF、pprof |
| observability | Prometheus、Grafana、OpenTelemetry、node exporter、cAdvisor |
| Kubernetes | kube-state-metrics、apiserver metrics、events |

### Substrate 必采集指标

已有或建议补充：

- `rpc.server.call.duration`
- `atelet.snapshot.size`
- actor lifecycle latency histogram。
- checkpoint/restore stage latency。
- worker assignment latency。
- worker state transition count。
- local snapshot hit/miss count。
- locality conflict count。
- no free workers count。
- Valkey operation latency/error。
- object storage operation latency/error。
- runsc checkpoint/restore exit code。

### 日志要求

每轮测试必须保存：

- ateapi logs。
- atelet logs。
- ateom-gvisor logs。
- runsc logs。
- Kubernetes events。
- Valkey logs。
- object storage client logs。
- benchmark runner logs。
- node dmesg/journal 中与 KVM、OOM、disk、network 相关的片段。

## 结果目录格式

建议每次运行输出到：

```text
results/<date>-<cluster>-<run-id>/
  environment.yaml
  runtime-matrix.yaml
  node-inventory.tsv
  workload-matrix.yaml
  benchmark-config.yaml
  manifest.json
  metrics/
    prometheus-snapshot.tar.zst
    prometheus-raw/
    otel-traces.jsonl.zst
    node-exporter.csv
  raw/
    locust-stats.csv
    lifecycle-events.jsonl
    cluster-scaling-events.jsonl
    fio/
    iperf3/
    sockperf/
    sysbench/
    syscall/
    object-storage/
    memory-gradient/
    substrate-api/
    kubernetes-api/
    cloud-api/
  derived/
    latency-percentiles.csv
    density-frontier.csv
    scale-trends.csv
    memory-gradient.csv
    snapshot-efficiency.csv
    correlation-analysis.csv
    regression-table.csv
  logs/
    ateapi/
    atelet/
    ateom-gvisor/
    runsc/
    kubernetes-events.jsonl
  profiles/
    pprof/
    perf/
    flamegraphs/
  process/
    runbook.md
    command-history.sh
    manual-steps.md
    issue-log.jsonl
    workaround-log.md
    environment-changes.jsonl
    rerun-decisions.md
    tool-requirements.md
    automation-gap-analysis.md
  reports/
    index.html
    assets/
    data/
      summary.json
      charts.json
      tables/
    summary.md
    report-metadata.json
```

目录原则：

- `raw/`、`metrics/`、`logs/`、`profiles/` 是原始证据层，必须保留，不能只保留聚合结果。
- `process/` 是过程证据层，记录人工步骤、问题、workaround、重跑决策和后续工具需求。
- `derived/` 是可重复生成的聚合数据，所有文件都必须能从原始证据层复算。
- `reports/` 是展示层，HTML 报告只能引用 `derived/` 或 `reports/data/` 中的数据，不能手工录入数字。
- `manifest.json` 必须记录每个文件的路径、大小、sha256、生成时间、生成工具版本和父级输入文件，保证后续可以做更多维度聚合和审计。

`environment.yaml` 至少包含：

```yaml
substrate:
  commit: ""
  image_tags: {}
cluster:
  provider: ack
  region: ""
  kubernetes_version: ""
  node_image: ""
nodes:
  instance_type: ""
  count: 0
  cpu_model: ""
  kernel: ""
  disk_type: ""
runtime:
  containerd: ""
  runc: ""
  runsc: ""
  gvisor_platform: ""
  kata: ""
  firecracker: ""
storage:
  snapshot_backend: oss
  endpoint_type: internal
  bucket_region: ""
network:
  cni: ""
  mtu: 0
```

### 结果字段 schema

`lifecycle-events.jsonl` 每行一个 stage event：

```json
{
  "run_id": "R2-ack-10n-substrate-gvisor-1000a-idle-heavy-20260714T120000Z",
  "operation_id": "uuid",
  "actor_id": "namespace/name",
  "actor_template": "namespace/name",
  "operation": "resume",
  "stage": "runsc_restore",
  "node": "node-name",
  "worker": "worker-pod-name",
  "start_ts": "2026-07-14T12:00:00.000Z",
  "end_ts": "2026-07-14T12:00:00.321Z",
  "duration_ms": 321,
  "snapshot_policy": "local-preferred",
  "snapshot_locality": "same-node",
  "memory_profile": "memory-random",
  "target_memory_bytes": 1073741824,
  "validated_rss_bytes": 1069547520,
  "validated_pss_bytes": 1061000000,
  "dirty_memory_ratio": 0.5,
  "snapshot_size_bucket": "1GiB",
  "snapshot_uri_prefix": "oss://bucket/prefix/actor/snapshot-id/",
  "snapshot_file_count": 4,
  "snapshot_logical_bytes": 1073741824,
  "snapshot_populated_bytes": 1069547520,
  "snapshot_compressed_bytes": 1040187392,
  "checkpoint_throughput_bps": 0,
  "restore_throughput_bps": 333000000,
  "workload": "idle-heavy",
  "result": "ok",
  "error_code": "",
  "error_reason": ""
}
```

`latency-percentiles.csv` 字段：

```text
run_id,operation,stage,workload,memory_profile,target_memory_bytes,snapshot_policy,snapshot_size_bucket,node_count,workers_per_node,actor_count,samples,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms,max_ms,error_rate
```

`density-frontier.csv` 字段：

```text
run_id,node_count,workers_per_node,actor_count,active_ratio,paused_local_ratio,paused_external_ratio,multiplexing_ratio,p95_resume_ms,p99_resume_ms,error_rate,cpu_utilization,memory_utilization,disk_iowait,network_rx_bps,network_tx_bps,valkey_p99_ms,kube_api_throttle_rate,oss_p99_ms,bottleneck
```

`regression-table.csv` 字段：

```text
base_run_id,candidate_run_id,metric,dimension,base_value,candidate_value,delta_percent,regression_threshold_percent,result
```

`scale-trends.csv` 字段：

```text
run_id,node_count,baseline,workload,snapshot_policy,worker_profile,actor_count,workers_total,multiplexing_ratio,throughput_ops_sec,p50_resume_ms,p95_resume_ms,p99_resume_ms,error_rate,local_snapshot_hit_rate,oss_get_p99_ms,oss_put_p99_ms,valkey_p99_ms,kube_api_throttle_rate,cpu_utilization,memory_utilization,disk_iowait,network_tx_bps,network_rx_bps,scale_efficiency,latency_growth,cost_efficiency,bottleneck
```

`memory-gradient.csv` 字段：

```text
run_id,node_count,workers_per_node,actor_count,workload,memory_profile,target_memory_bytes,validated_rss_bytes,validated_pss_bytes,dirty_memory_ratio,snapshot_policy,snapshot_locality,snapshot_size_bucket,samples,checkpoint_p50_ms,checkpoint_p95_ms,checkpoint_p99_ms,restore_p50_ms,restore_p95_ms,restore_p99_ms,cross_node_restore_p99_ms,snapshot_logical_bytes,snapshot_populated_bytes,snapshot_compressed_bytes,compression_ratio,checkpoint_throughput_bps,restore_throughput_bps,cpu_utilization,memory_utilization,disk_iowait,oss_get_p99_ms,oss_put_p99_ms,error_rate,valid_sample_rate,bottleneck
```

`snapshot-efficiency.csv` 字段：

```text
run_id,actor_id,operation_id,node,source_node,target_node,worker,target_worker,memory_profile,target_memory_bytes,validated_rss_bytes,dirty_memory_ratio,snapshot_uri_prefix,snapshot_file_count,snapshot_logical_bytes,snapshot_populated_bytes,snapshot_compressed_bytes,checkpoint_duration_ms,upload_duration_ms,download_duration_ms,decompress_duration_ms,runsc_restore_duration_ms,business_ready_duration_ms,checkpoint_throughput_bps,restore_throughput_bps,compression_ratio,result,error_code,error_reason
```

`correlation-analysis.csv` 字段：

```text
run_id,node_count,workload,metric_x,metric_y,window_seconds,correlation_method,correlation_value,lag_seconds,interpretation
```

### 原始数据留存要求

所有正式 run 必须保留原始测试数据，方便后续按新维度重新聚合。任何报告中的数字都必须能追溯到原始数据。

原始数据至少包括：

| 数据源 | 原始文件 | 用途 |
|---|---|---|
| benchmark runner | `raw/locust-stats.csv`、runner events JSONL | 重新计算 QPS、错误率、端到端延迟 |
| actor lifecycle | `raw/lifecycle-events.jsonl` | 重新聚合 create/pause/resume/commit stage latency |
| cluster scaling | `raw/cluster-scaling-events.jsonl` | 关联扩缩容事件和性能抖动 |
| microbench | `raw/fio/`、`raw/iperf3/`、`raw/sysbench/`、`raw/syscall/` | 重新计算 runtime baseline |
| object storage | `raw/object-storage/` | 重新分析 OSS GET/PUT/HEAD/LIST |
| memory gradient | `raw/memory-gradient/`、`raw/substrate-api/lifecycle-events*.jsonl` | 重新分析目标内存、实际 RSS/PSS、dirty ratio、快照大小和恢复效率 |
| Substrate API | `raw/substrate-api/` | 复算 ateapi client-side latency 和 error |
| Kubernetes API | `raw/kubernetes-api/` | 复算 API throttling、watch lag、event storm |
| cloud API | `raw/cloud-api/` | 复算 ACK 扩缩容、ECS、ACR、OSS 控制面事件 |
| Prometheus | `metrics/prometheus-raw/` 或 snapshot | 重新计算任意时间窗口指标 |
| traces | `metrics/otel-traces.jsonl.zst` | 重新做跨 stage trace 分析 |
| logs | `logs/` | 复盘错误语义和异常事件 |
| profiles | `profiles/` | 复盘 CPU/内存/锁/系统调用瓶颈 |

留存要求：

- 原始文件只追加写入，不做人工编辑。
- 原始文件压缩可以接受，但必须保留 schema 和解压方式。
- 每个原始文件必须进入 `manifest.json`，记录 sha256。
- 派生文件必须记录生成命令、输入文件 sha256 和生成工具版本。
- 如果原始数据缺失，相关 run 不能标记为 blessed。
- 如果后续报告发现新问题，应优先从 raw 数据重新聚合，不要求重跑测试，除非原始采集维度不足。

## 判读方法

### 不直接使用单个平均值

所有结论必须以 percentile、分布、错误率和资源曲线为主。平均值只能用于辅助说明。

### 统计和有效性规则

| 规则 | 要求 |
|---|---|
| warmup | 正式采样前至少 5 分钟或完成 100 次生命周期操作，取更大者 |
| cooldown | 每个 run 结束后保留至少 2 分钟冷却窗口，用于确认异步错误和资源回落 |
| repeats | smoke 至少 3 次，baseline 至少 5 次，scale 至少 3 次，soak 至少 1 次 |
| 最小样本量 | P99 结论每个分桶至少 1000 个样本；P999 结论至少 10000 个样本 |
| 置信区间 | 关键指标报告 bootstrap 95% confidence interval |
| 失败样本 | timeout、5xx、调度失败必须计入 error rate；若请求有明确开始时间，也应计入 latency 分布或单独报告 timeout bucket |
| 异常值 | 不删除异常值；只能在附录中提供 winsorized 辅助视图 |
| 随机种子 | workload 分布、actor 选择、故障注入目标必须记录 seed |
| 时钟 | 跨节点 stage latency 只在时钟同步达标后使用；否则只使用单进程 monotonic duration |
| 数据完整性 | metrics/logs/traces 任一关键数据缺失超过 1%，该 run 标记为 partial |
| 内存有效性 | memory-gradient run 中 `validated_rss_bytes / target_memory_bytes < 0.95` 的样本必须标记 invalid，不能进入 percentile |
| 快照尺寸有效性 | memory-gradient、cross-node recover、external restore run 缺少 `snapshot_populated_bytes` 或 `snapshot_compressed_bytes` 时不能发布恢复效率结论 |

每轮测试后必须执行一次数据质量 review，确认是否进入下一轮。没有通过数据质量 review 的结果不能用于趋势判断。

### 建议 SLO

初始建议，可在第一次测试后校准：

| 指标 | 初始目标 |
|---|---|
| warm local resume P95 | < 500 ms |
| warm local resume P99 | < 1000 ms |
| external restore P95 | < 5 s，按 snapshot size 分桶 |
| pause memory-only P95 | < 2 s，按 memory size 分桶 |
| API error rate | < 0.1% |
| worker assignment P99 | < 100 ms |
| local snapshot hit rate | > 90%，在 locality enabled 场景 |
| 24h soak latency drift | P99 不超过初始窗口 2 倍 |

这些不是最终性能承诺，只是第一轮测试的红线。最终 SLO 应基于真实业务 workload 调整。

SLO 必须按 workload 和 snapshot size 分桶解释：

| workload | snapshot bucket | warm local resume P95 | external restore P95 | 备注 |
|---|---|---:|---:|---|
| idle-heavy | <=256MiB | < 500 ms | < 3 s | 第一轮主要 SLO |
| tool-heavy | <=256MiB | < 750 ms | < 5 s | syscall 和小文件较多 |
| code-heavy | 1GiB | < 1500 ms | < 15 s | durable dir 和 package cache 影响大 |
| memory-heavy | 4GiB | < 3 s | 以吞吐曲线判断 | 不强设固定秒级目标 |
| memory-heavy | 16GiB | 以曲线判断 | 以曲线判断 | 用于寻找不可接受拐点 |
| chat-serving | <=256MiB | < 500 ms | < 3 s | 同时关注首包延迟和 streaming 稳定性 |

对于 4GiB 以上 snapshot，不能只用固定秒级 SLO 判断，应报告 restore throughput、object storage bandwidth、CPU decompression/copy 成本和 P99 曲线。

内存梯度测试的第一轮目标不是强行给 4GiB/8GiB/16GiB 设统一秒级 SLO，而是建立斜率和拐点：

| 维度 | 必须报告 |
|---|---|
| 小内存基线 | 64MiB、256MiB 的 cold/warm/local/external restore P50/P95/P99 |
| 中内存曲线 | 512MiB、1GiB、2GiB 的 checkpoint/restore throughput 和 P99 growth per GiB |
| 大内存拐点 | 4GiB、8GiB、16GiB 中第一个 error rate、OOM、OSS 限流或 P99 非线性增长的档位 |
| 可压缩性影响 | zeroed/random/agent-cache 三类 profile 的 snapshot compressed bytes 和 restore P99 差异 |
| 跨节点惩罚 | same-node、same-node different-worker、cross-node external restore 的 P99 比值 |
| 规模衰减 | 10/50/100 节点下同一内存档位的 P99、throughput、error rate 和资源利用率 |

### density frontier

最终报告需要画出密度边界：

- x 轴：actors per node 或 actors per cluster。
- y 轴：P95/P99 resume latency。
- 颜色：error rate。
- 标记：CPU、memory、disk、network、Valkey、Kubernetes API 中最先达到瓶颈的资源。

结论应回答：

- 单节点可承载多少 active actor。
- 单节点可承载多少 paused actor。
- 10/50/100 节点时控制面是否线性扩展。
- 哪个资源最先限制规模。
- local snapshot 命中降低了多少恢复延迟。
- external snapshot 在多大 snapshot size 后不可接受。

### 瓶颈归因决策表

| 现象 | 可能瓶颈 | 需要同时满足的证据 |
|---|---|---|
| resume P99 上升，CPU util > 85% | node CPU 或 gVisor sentry CPU | per-component CPU 上升，runsc restore stage 变慢 |
| resume P99 上升，OSS P99 上升 | external snapshot/object storage | external restore 分桶变慢，local restore 不变 |
| pause/commit 变慢，disk iowait > 20% | local snapshot disk | atelet checkpoint stage 变慢，本地盘 await 上升 |
| API latency 上升，Valkey P99 上升 | Valkey | Valkey ops/sec 或 memory 接近上限，ateapi stage 变慢 |
| 调度失败增加，worker cache age 上升 | worker cache/state sync | no free workers 与真实 idle worker 不一致 |
| 所有 Kubernetes 操作变慢 | Kubernetes API throttling | apiserver throttle、client-go retry、watch lag 同时出现 |
| cold start 变慢，image pull 变慢 | registry/ACR | pull latency 和 registry error 上升，warm start 不受影响 |
| only cross-node restore 变慢 | 网络或 locality | same-node local restore 稳定，cross-node/OSS path 变慢 |

## 分析报告和图表需求

最终报告必须以图表和趋势分析为主，不能只给表格和文字结论。每个图表都必须标注 run-id、样本数、节点规模、workload、snapshot policy、worker profile 和 run 状态。

### 必备图表

| 图表 | x 轴 | y 轴 | 分组/颜色 | 目的 |
|---|---|---|---|---|
| 节点规模趋势图 | node count：10/50/100 | P50/P95/P99 resume latency | sandbox/runtime、workload | 看规模扩大后延迟是否线性或非线性退化 |
| 密度边界图 | actors per node / actors per cluster | P95/P99 resume latency | error rate 或 bottleneck | 找到每个规模的可承载边界 |
| throughput scaling 图 | node count | successful actor ops/sec | workload、snapshot policy | 计算 scale efficiency |
| error rate 趋势图 | time 或 actor count | error rate | error reason | 判断错误是否随规模集中爆发 |
| snapshot size 曲线 | snapshot size bucket | checkpoint/restore P95/P99 | local/external | 判断 snapshot size 拐点 |
| 内存梯度效率曲线 | target memory bucket | checkpoint/restore throughput、P95/P99 | memory profile、dirty ratio | 判断内存增长后效率是否线性下降 |
| 快照压缩率曲线 | target memory bucket | compressed/populated ratio | zeroed/random/agent-cache | 区分可压缩小快照和真实大状态 |
| 跨节点恢复衰减图 | target memory bucket | cross-node restore P99 / same-node restore P99 | 10/50/100 节点 | 判断跨节点 recover 随内存和规模的惩罚 |
| 资源放大曲线 | target memory bucket | node memory delta、CPU、disk iowait、network bps | local/external | 判断每 GiB actor 状态带来的资源放大 |
| local hit rate 图 | actor count 或 time | local snapshot hit rate | node count | 判断 locality 是否随规模退化 |
| object storage 影响图 | OSS GET/PUT P99 | restore/commit P99 | local/external | 判断 OSS 是否驱动端到端延迟 |
| Valkey 联动图 | Valkey P99 / ops/sec | API P99 / assignment P99 | node count | 判断控制面状态存储瓶颈 |
| worker cache 图 | worker cache age / relist count | assignment failures | node count | 判断 cache staleness 对调度的影响 |
| resource heatmap | node count x actor count | CPU/memory/disk/network utilization | bottleneck | 看资源瓶颈从哪里出现 |
| sandbox 对比雷达图 | runtime dimension | normalized overhead | runc/gVisor/Kata/Firecracker-ref | 展示不同沙箱在 CPU/syscall/fs/net/startup 的取舍 |
| scale-out timeline | time | nodes ready、workers ready、P99、error rate | scaling event | 展示 API 扩容期间系统是否抖动 |
| cost efficiency 图 | node count | successful ops per cost unit | workload | 判断 50/100 节点是否经济 |

### 10/50/100 节点联动趋势

报告必须给出三个标准规模的联动分析，而不是分别描述每个规模。

| 分析项 | 10 节点 | 50 节点 | 100 节点 | 必须回答的问题 |
|---|---|---|---|---|
| latency | P50/P95/P99/P999 | P50/P95/P99/P999 | P50/P95/P99/P999 | P99 是否随节点数增长，增长来自哪里 |
| throughput | ops/sec | ops/sec | ops/sec | 吞吐是否接近线性扩展 |
| density | max actors/node、max actors/cluster | 同左 | 同左 | 承载边界是否随规模下降 |
| local snapshot | hit/miss reason | hit/miss reason | hit/miss reason | locality 是否在大规模下降 |
| control plane | ateapi、Valkey、Kubernetes API | 同左 | 同左 | 控制面是否成为 50/100 节点瓶颈 |
| data plane | actor QPS、首包延迟、连接错误 | 同左 | 同左 | 数据面是否被路由或网络放大 |
| memory gradient | 256MiB/1GiB/4GiB P99 和 throughput | 同左 | 同左 | 同样内存档位在大规模下是否衰减 |
| cloud deps | OSS、ACR、NAT/SLB | 同左 | 同左 | 云服务是否出现限流或跨区带宽瓶颈 |
| cost | 节点小时、OSS、日志/metrics | 同左 | 同左 | 单位成功操作成本是否恶化 |

必须包含两类对比：

- 同 actor 数对比：例如 10000 actor 在 10/50/100 节点下的延迟和资源变化。
- 同每节点密度对比：例如每节点 500 actor 时，10/50/100 节点下的控制面和云依赖变化。

### 沙箱规模变化分析

不同沙箱或 sandbox profile 在不同规模下要分别观察，不能只给单节点 microbench。

| 维度 | 10 节点 | 50 节点 | 100 节点 |
|---|---|---|---|
| runc RuntimeClass | native Kubernetes baseline | 如果可用，作为低隔离开销扩展对照 | 如果成本允许，保留关键 workload |
| gVisor systrap | 非 KVM fallback 对照 | 观察 syscall/fs overhead 是否放大 | 仅在必要时跑关键子集 |
| gVisor KVM | Substrate 目标路径 | 主要 scale baseline | 主要 scale baseline |
| Kata | VM-based 对照 | 观察 boot/memory/density | 可选关键子集 |
| Firecracker-ref | 行业参考 | 不进入 Substrate 主排名 | 不进入 Substrate 主排名 |

报告需要展示：

- 单节点 overhead 是否会在 10/50/100 节点下放大。
- gVisor KVM 的瓶颈是否从 syscall/filesystem 转移到 control plane、OSS 或 worker cache。
- sandbox startup overhead 与 Substrate warm resume 是否属于同一瓶颈。
- 资源占用随 sandbox 数量增长是否线性。

### 联动趋势分析

报告必须给出跨指标相关性，不允许只列独立指标。

必做联动分析：

| 关系 | 目的 |
|---|---|
| resume P99 vs local snapshot hit rate | 判断恢复延迟是否由 locality 驱动 |
| restore P99 vs OSS GET P99 / bytes | 判断 external restore 是否由对象存储驱动 |
| restore P99 vs target memory / populated bytes | 判断恢复延迟是否随真实内存尺寸线性增长 |
| checkpoint P99 vs dirty memory ratio | 判断脏页比例是否驱动 checkpoint 成本 |
| compression ratio vs memory profile | 判断快照很小是不是因为数据过度可压缩 |
| cross-node penalty vs snapshot compressed bytes | 判断跨节点 recover 是否被网络和对象存储大小驱动 |
| commit P99 vs OSS PUT P99 / local disk iowait | 判断 checkpoint 写入瓶颈 |
| assignment P99 vs worker cache age | 判断调度延迟是否由 cache stale 驱动 |
| API P99 vs Valkey P99 | 判断控制面状态存储是否瓶颈 |
| error rate vs Kubernetes API throttling | 判断 API Server 限流是否导致失败 |
| cold start P99 vs registry pull P99 | 判断 ACR/registry 是否瓶颈 |
| P99 drift vs ateapi/atelet RSS | 判断 soak 中是否有资源泄漏 |
| throughput vs node count | 判断 scale efficiency |
| cost per successful operation vs node count | 判断扩容是否经济 |

联动分析需要包含时间滞后。比如 OSS GET P99 先升高 30 秒，随后 restore P99 升高，这比同一时间窗口的相关性更有解释力。`correlation-analysis.csv` 中的 `lag_seconds` 用于记录这个关系。

### 报告分层

每轮报告分三层：

1. Overview dashboard：给决策者看，展示 10/50/100 节点趋势、SLO 是否通过、第一瓶颈、成本。
2. Engineering dashboard：给研发看，展示 stage latency、resource heatmap、worker cache、Valkey、OSS、Kubernetes API。
3. Raw evidence appendix：给复盘和社区 issue 使用，包含 run-id、CSV、trace/log/profile 链接。

任何最终结论都必须能从 Overview drill down 到 Engineering，再 drill down 到 raw evidence。

### HTML 报告标准

最终报告必须生成 HTML 格式，入口为 `reports/index.html`。Markdown 只能作为辅助摘要，不能替代 HTML 报告。

HTML 报告要求：

| 要求 | 说明 |
|---|---|
| 单一入口 | `reports/index.html` 可以离线打开，或部署为静态站点 |
| 标准布局 | 所有 run 使用相同页面结构、图表 ID、表格字段和颜色规范 |
| 数据驱动 | 图表从 `reports/data/*.json` 或 `derived/*.csv` 加载，不允许手工写死数字 |
| 可追溯 | 每张图都显示 run-id、数据文件、生成时间和 raw evidence 链接 |
| 可交互 | 支持按 node count、runtime、workload、snapshot policy、worker profile 过滤 |
| 可对比 | 支持 10/50/100 节点并排对比和多 run overlay |
| 可导出 | 图表可导出 PNG/SVG，表格可导出 CSV |
| 可复现 | `report-metadata.json` 记录报告生成工具、版本、输入文件 sha256 |
| 可降级 | 如果浏览器禁用 JS，至少能看到 summary tables 和 raw data index |

推荐 HTML 页面结构：

```text
reports/
  index.html
  assets/
    report.css
    report.js
    charts.js
  data/
    summary.json
    charts.json
    raw-index.json
    tables/
      latency-percentiles.json
      density-frontier.json
      scale-trends.json
      correlation-analysis.json
  report-metadata.json
```

推荐图表技术可以是任意静态 HTML 兼容方案，例如 Vega-Lite、Plotly、ECharts 或 D3。选择标准是：离线可打开、图表配置可版本化、数据文件可替换、导出能力稳定。

HTML 报告必须包含 raw data index 页面：

- 列出本次 run 的所有 raw、metrics、logs、profiles 文件。
- 显示文件大小、sha256、时间范围、schema 版本。
- 显示每个 derived/report 文件来自哪些 raw 输入。
- 标记缺失、partial、aborted、contaminated 数据。

报告生成流程必须是幂等的：同一批 raw 数据和同一版本生成工具，多次生成的 `derived/` 和 `reports/` 内容应一致。

## 从首次验收到自动化工具

本项目分成两个交付阶段。第一阶段目标是得到可信的完整性能测试报告和原始数据，不把工具研发放在关键路径上；第二阶段再基于第一阶段记录的真实过程、问题和数据形态，研发可重复运行的性能测试工具，并在新环境中完成调试和验收。

### 阶段 A：首次完整性能测试

目标：

- 完成 R0 到 R4 的完整性能测试。
- 交付 HTML 性能测试报告。
- 交付完整原始数据归档。
- 记录所有人工步骤、环境变更、失败重试、workaround 和问题。

边界：

- 不在首次完整测试期间开发通用性能测试工具。
- 可以使用临时脚本、手工命令和现有 benchmark harness，但必须记录。
- 所有临时脚本和命令都要进入过程归档，作为后续工具化输入。
- 测试中遇到的问题不直接隐藏在最终结论里，必须进入 issue log。

必须额外留存：

```text
results/<date>-<cluster>-<run-id>/
  process/
    runbook.md
    command-history.sh
    manual-steps.md
    issue-log.jsonl
    workaround-log.md
    environment-changes.jsonl
    rerun-decisions.md
    tool-requirements.md
```

`issue-log.jsonl` 每行记录：

```json
{
  "run_id": "R2-ack-10n-substrate-gvisor-5000a-tool-heavy-20260714T120000Z",
  "issue_id": "perf-issue-001",
  "time": "2026-07-14T12:30:00.000Z",
  "severity": "P1",
  "category": "observability",
  "symptom": "restore P99 increased but stage attribution was missing for 2% requests",
  "impact": "run marked partial",
  "workaround": "reran after fixing trace sampling",
  "tooling_requirement": "runner must fail the run when stage coverage < 99%",
  "status": "closed"
}
```

阶段 A 结束时必须产出：

- 完整 HTML 性能测试报告。
- 完整 raw/metrics/logs/profiles/process 归档。
- `tool-requirements.md`：从真实测试过程中提炼出的自动化工具需求。
- `automation-gap-analysis.md`：哪些步骤可以自动化，哪些需要人工审批，哪些依赖云资源状态。

### 阶段 B：自动化工具研发和新环境验收

阶段 B 在阶段 A 完成后启动。启动前必须清理存量测试环境，并创建或切换到新的干净测试环境。工具验收必须在新环境完成，避免把首次测试中的临时状态、缓存、残留 snapshot、残留镜像或手工配置当成工具能力。

目标：

- 构建一个可以再次执行性能测试并输出标准 HTML 报告的工具。
- 工具能从配置生成 run matrix，执行测试，采集 raw 数据，生成 derived 数据，输出 HTML 报告。
- 工具能执行环境 preflight、运行中中止条件、数据完整性检查、归档和清理。
- 工具在新环境中完成至少 Day-1 smoke 和 Week-1 baseline 验收。

工具最小能力：

| 能力 | 要求 |
|---|---|
| config-driven | 从 YAML/JSON 配置读取 cluster、runtime、worker profile、workload、run matrix |
| environment preflight | 检查 ACK、节点、KVM、OSS、ACR、Prometheus、权限、配额 |
| scale control | 通过 API 执行 10/50/100 节点扩缩容，并记录 `cluster-scaling-events.jsonl` |
| run orchestration | 按 run matrix 执行 R0/R1/R2/R3/R4/R5 子集 |
| data collection | 采集 raw、metrics、logs、profiles、process 记录 |
| abort handling | 按中止条件停止 run，标记 aborted 并保留现场 |
| derivation | 从 raw 生成 derived CSV/JSON |
| HTML report | 生成 `reports/index.html` 和 dashboard 数据 |
| archive | 生成 `manifest.json`，计算 sha256，上传长期归档 |
| cleanup | 清理 actor、worker、namespace、OSS 临时 prefix、临时镜像 tag、临时节点池 |
| rerun | 支持基于同一配置重复运行并生成可比 run-id |

建议工具目录边界：

```text
benchmarking/perfkit/
  README.md
  config.example.yaml
  run_matrix.yaml
  cmd/
  internal/
  reports/
    templates/
    assets/
  collectors/
  analyzers/
  cloud/
    ack/
  cleanup/
```

阶段 B 验收标准：

- 在新环境中从空白状态完成 preflight。
- 自动扩容到 10 节点并完成 Day-1 smoke。
- 自动生成 `results/<run-id>/manifest.json`。
- 自动生成 `reports/index.html`，且可离线打开。
- raw 数据、derived 数据和 HTML 图表数字一致。
- 清理后没有遗留 actor、temporary namespace、OSS 临时 prefix、测试节点池或临时镜像 tag。
- 同一配置重复运行两次，run 状态、目录结构和报告 schema 一致。

### 最终交付物

完整工作结束时必须交付两个东西：

1. 完整性能测试报告和原始数据：
   - `reports/index.html`
   - `manifest.json`
   - `raw/`
   - `metrics/`
   - `logs/`
   - `profiles/`
   - `process/`
   - `derived/`
2. 可再次运行性能测试的工具：
   - 工具源码。
   - 配置样例。
   - 运行文档。
   - 清理文档。
   - 新环境验收报告。
   - 工具自身生成的一份标准 HTML 报告。

如果两个交付物中任意一个未完成，则整体工作不能标记完成。

## 多轮执行模型

性能测试必须按轮次推进。每一轮结束后都要产出 review notes，并决定下一轮是扩大规模、收窄变量、补 instrumentation，还是回退重跑。

### R0：smoke 和测量系统校准

目的：验证工具链、schema、指标、日志、trace 和最小 workload，不追求性能结论。

输入：

- 1 节点或最小 ACK node pool。
- `smoke` worker profile。
- runc vs gVisor KVM microbench。
- 100 actor Substrate lifecycle。

输出：

- result schema 可解析。
- lifecycle stage trace 覆盖率。
- metrics/logs/traces 完整性。
- 首版 dashboard。

晋级条件：

- 测试准入门槛全部通过。
- 每个正式指标都能找到采集来源。
- run matrix 至少能生成 R1 所需配置。

如果失败：

- 不进入 R1。
- 优先修复 instrumentation、runner schema、KVM readiness、OSS/registry probe。

### R1：单节点 baseline

目的：分离 runtime overhead 和 Substrate lifecycle overhead。

输入：

- host、runc、gVisor systrap、gVisor KVM、Kata RuntimeClass。
- 1 节点 Substrate + gVisor。
- 100/500/1000 actor 阶梯。
- 4/8/16 workers per node。

输出：

- runtime microbench 表。
- gVisor KVM vs systrap 差异。
- 单节点 actor lifecycle 分桶。
- worker profile 初始建议。

晋级条件：

- P99 样本量达标。
- 单节点瓶颈可解释。
- worker profile 对 R2 规模测试足够稳定。

如果失败：

- 收窄 workload。
- 降低 actor 阶梯。
- 补充 stage 指标后重跑 R1。

### R2：10 节点 ACK baseline

目的：建立 ACK 上第一条可用的密度和 snapshot 曲线。

输入：

- 10 节点 ACK。
- `baseline` worker profile。
- 100/500/1000/2500/5000/10000 actor。
- idle-heavy、tool-heavy、code-heavy。
- local-preferred vs external-only。

输出：

- 10 节点 density frontier。
- snapshot locality hit/miss 分析。
- OSS/ACR/KVM/网络对性能的影响。
- ACK 配额触达情况。

晋级条件：

- 至少一个 workload 达到 5000 actor 且 P99 可归因。
- local vs external snapshot 差异清晰。
- 没有未解释的控制面抖动。

如果失败：

- 如果是数据缺失，回 R0/R1 补 instrumentation。
- 如果是云配额，调整 ACK/OSS/ACR 配额或降低规模。
- 如果是 Substrate 瓶颈，记录为 community optimization input，再重跑局部矩阵。

### R3：突发和故障注入

目的：验证在高并发生命周期操作和受控故障下，系统是否有可解释的退化和恢复行为。

输入：

- 10 或 50 节点。
- burst worker profile。
- 1000/5000/10000 actor resume burst。
- ateapi、atelet、Valkey、node drain、OSS timeout、registry slow 等受控故障。

输出：

- burst saturation point。
- queue time 和 assignment failure reason。
- 故障恢复矩阵。
- 数据丢失、重复 assignment、orphan worker 检查。

晋级条件：

- 每类故障都有明确 error semantics。
- 恢复时间和失败率可量化。
- 没有无法解释的一致性问题。

如果失败：

- 固定失败场景，单独生成 bug/optimization report。
- 不直接进入 R4 大规模测试。

### R4：50/100 节点 scale 和 soak

目的：验证扩展边界、长稳、资源泄漏和控制面线性扩展能力。

输入：

- 50 到 100 节点。
- density/soak worker profile。
- 10000/25000/50000/100000 actor。
- 12 到 24 小时 soak。

输出：

- 规模曲线。
- density frontier。
- 24 小时 latency drift。
- ateapi/atelet/Valkey memory 曲线。
- snapshot object 和 local disk 增长曲线。

晋级条件：

- 关键指标在 12/24 小时内无无法解释的持续漂移。
- P99 和 error rate 在目标规模内可接受。
- 清理和保留策略验证通过。

如果失败：

- 按瓶颈归因表缩小变量。
- 重新执行 R2 或 R3 的相关子集。

### R5：回归固化

目的：把探索性测试转成持续回归，判断 Substrate commit 或 gVisor/runsc 版本变更是否退化。

输入：

- 固定 1 节点和 10 节点小矩阵。
- runc vs gVisor KVM runtime baseline。
- 100/1000/5000 actor lifecycle。
- idle-heavy、tool-heavy、local-preferred snapshot。

输出：

- nightly 或 weekly regression table。
- 与上一个 blessed run 的 delta。
- 性能退化 gate。

建议 gate：

- P95/P99 resume latency 退化 > 10% 标记 warning。
- P95/P99 resume latency 退化 > 20% 标记 failure。
- error rate 增加 > 0.5 个百分点标记 failure。
- local snapshot hit rate 下降 > 5 个百分点标记 failure。

## 最小可执行测试集

如果时间有限，按三档执行，不要把所有内容压进第一轮。

### Day-1 smoke

只回答“测试系统是否可信”：

1. runc vs gVisor KVM microbenchmark：
   - sysbench CPU。
   - fio sequential/random。
   - iperf3。
   - custom syscall bench。
2. Substrate lifecycle 1 节点：
   - 100 actor。
   - create/pause/resume/commit。
   - local-preferred snapshot。
3. schema 和 telemetry：
   - `lifecycle-events.jsonl` 可解析。
   - percentile CSV 可生成。
   - trace/log/metrics 可按 run-id 关联。

### Week-1 baseline

回答“10 节点内性能边界在哪里”：

1. 单节点 runtime baseline：
   - host、runc、gVisor systrap、gVisor KVM、Kata。
2. Substrate lifecycle 10 节点：
   - 100、500、1000、2500、5000 actor。
   - idle-heavy、tool-heavy、code-heavy。
   - local-preferred vs external-only。
3. snapshot size curve：
   - <=256MiB、1GiB、4GiB。
4. burst 小规模：
   - 1000 resume in 60 秒。

### Full scale

回答“是否达到生产规模目标”：

1. 50/100 节点 scale：
   - 10000、25000、50000、100000 actor。
2. 受控故障注入：
   - ateapi、atelet、Valkey、node drain、OSS timeout、registry slow。
3. 12/24 小时 soak：
   - 50000 actor 起步。
   - mixed-agent workload。
4. regression 固化：
   - 选出 R5 nightly/weekly 子集。

每一档结束后都要更新下一档矩阵。不能在 Day-1 结果不可信时直接进入 Week-1，也不能在 10 节点瓶颈未归因时直接进入 100 节点。

## 节点规模和 API 驱动扩展

节点规模不是固定测试常量。默认规模为 10 节点，用于形成 ACK baseline；后续必须通过 API 或自动化控制面扩展到 50 和 100 节点，并把扩缩容事件纳入结果分析。

### 标准节点规模阶梯

| 规模 | 用途 | actor 阶梯 | 主要问题 |
|---|---|---|---|
| 10 节点 | 默认 baseline | 100/500/1000/2500/5000/10000 | 判断 Substrate + gVisor 在已验收 ACK 环境上的基础性能和第一瓶颈 |
| 50 节点 | 中等规模扩展 | 10000/25000/50000 | 判断控制面、Valkey、worker cache、OSS/ACR 是否开始非线性退化 |
| 100 节点 | 大规模边界 | 50000/100000+ | 判断系统扩展上限、云配额和成本拐点 |

### 扩缩容事件记录

通过 API 扩缩容时，每次节点池变化必须输出 `cluster-scaling-events.jsonl`：

```json
{
  "run_id": "R4-scale-substrate-gvisor-50n-50000a-mixed-agent-20260714T120000Z",
  "event_id": "uuid",
  "operation": "scale_out",
  "nodepool": "substrate-c9i-manual",
  "from_nodes": 10,
  "to_nodes": 50,
  "requested_at": "2026-07-14T12:00:00.000Z",
  "ready_at": "2026-07-14T12:18:00.000Z",
  "duration_seconds": 1080,
  "api": "ack/nodepool-scale",
  "instance_type": "ecs.c9i.8xlarge",
  "new_nodes_ready": 40,
  "new_nodes_failed": 0,
  "kvm_ready_nodes": 40,
  "image_warmup_completed_nodes": 40,
  "result": "ok",
  "error_reason": ""
}
```

扩容完成不等于测试可开始。正式采样前必须等待：

- 所有新增节点 `Ready`。
- `/dev/kvm` 可用。
- atelet Ready。
- worker pool 达到目标 workers/node。
- ACR/registry 镜像 warmup 完成，或明确标记为 cold run。
- OSS probe 正常。
- metrics target 全部被 Prometheus scrape。

### 扩展趋势口径

10/50/100 节点必须使用相同 workload、worker profile、snapshot policy 和采样规则，除非 run matrix 明确记录变更。报告需要同时给出：

- fixed actor count：相同 actor 数在 10/50/100 节点下的延迟变化，用于看资源余量。
- proportional actor count：按节点数线性增加 actor，用于看扩展效率。
- saturation sweep：在每个节点规模内递增 actor，找到该规模的 density frontier。
- scale-out impact：扩容期间和扩容后 30 分钟内的 P95/P99、error rate、worker readiness、image pull、KVM readiness 变化。

扩展效率建议计算：

```text
scale_efficiency = throughput_at_N_nodes / (throughput_at_10_nodes * N / 10)
latency_growth = p99_resume_at_N_nodes / p99_resume_at_10_nodes
cost_efficiency = successful_actor_operations / total_cost
```

其中 `N` 为 50 或 100。若 `scale_efficiency < 0.8` 或 `latency_growth > 2.0`，必须进入瓶颈归因。

## 成本和资源预算

每轮测试前必须建立预算上限，并写入 run matrix。预算不是财务附录，而是测试停止条件的一部分。

| 轮次 | 主要成本 | 必填预算项 |
|---|---|---|
| R0 | 少量 ECS、少量 OSS 请求 | 节点小时、OSS 请求数、日志量 |
| R1 | 单节点重复测试、profiling 存储 | 节点小时、profile 大小、Prometheus retention |
| R2 | 10 节点、snapshot PUT/GET、镜像拉取 | 节点小时、OSS 存储量、OSS 请求数、ACR pull 次数 |
| R3 | 故障注入和 burst 产生的重试 | 节点小时、失败重试上限、日志量上限 |
| R4 | 50/100 节点、12/24h soak、大量 snapshot | 最大节点小时、最大 OSS 存储量、最大 OSS 请求数、最大日志/metrics 存储 |
| R5 | 持续回归 | 每周预算、数据保留周期、自动清理策略 |

建议在 `benchmark-config.yaml` 中记录：

```yaml
budget:
  max_node_hours: 0
  max_oss_storage_gib: 0
  max_oss_requests: 0
  max_registry_pulls: 0
  max_metrics_storage_gib: 0
  max_log_storage_gib: 0
  stop_when_budget_exceeded: true
```

## 测试环境隔离

正式 run 期间需要固定环境，避免 P99 被无关噪声污染。

- 不进行节点池扩缩容，除非该 run 专门测试 autoscaling。
- 不在同一集群运行无关 workload。
- 不在测试窗口内批量构建或推送镜像。
- 不调整 CNI、CoreDNS、ACK 托管组件、node image。
- 不变更 OSS bucket lifecycle、ACR 权限、registry mirror。
- 日志采集和 profiling 采样率必须提前固定。
- 记录所有云侧维护事件、节点重启、Kubernetes event storm。

如果环境发生不可控变更，该 run 标记为 contaminated，只能作为排障材料，不能进入正式对比。

## 数据保留和清理

性能测试会产生大量 snapshot、metrics、logs 和 profiles。每轮测试必须有清理策略，但原始测试数据必须优先长期留存。清理策略只能删除可重建的派生数据或明确不再需要的临时运行态资源，不能破坏后续多维度聚合能力。

| 数据 | 默认保留 | 清理要求 |
|---|---|---|
| raw benchmark CSV/JSONL | 长期 | 压缩后保留，必须进入 manifest |
| raw API/cloud/object-storage events | 长期 | 压缩后保留，必须进入 manifest |
| raw microbench output | 长期 | 保留工具原始输出和转换后的 CSV/JSON |
| summary reports | 长期 | HTML、metadata、summary 不删除 |
| derived CSV/JSON | 长期或可重建 | 若删除，必须能从 raw 重新生成 |
| Prometheus raw/snapshot | 180 天，关键 run 长期 | 大规模 run 可压缩归档；不能只保留截图 |
| traces | 90 到 180 天，关键 run 长期 | 按 run-id 压缩归档 |
| logs | 90 到 180 天，aborted run 更久 | 压缩后保留错误相关窗口 |
| pprof/perf/flamegraph | 90 天，瓶颈 run 长期 | 只保留关键 profile 也要进入 manifest |
| process records | 长期 | runbook、issue log、command history、automation gap 必须保留 |
| external snapshots | 7 到 30 天 | 除 golden sample 外自动删除 |
| local snapshots | run 后清理 | 验证 GC 后删除 |

每轮结束必须确认：

- actor、worker、temporary namespace 已清理。
- OSS 临时 prefix 已按策略删除。
- ACR 临时镜像 tag 已清理或标记。
- Prometheus/log storage 没有超出预算。
- raw 数据已经上传到长期归档位置。
- process 记录已经上传到长期归档位置。
- `manifest.json` 中所有 sha256 校验通过。
- HTML 报告可以从归档数据重新生成。

## 风险和注意事项

1. 不同云厂商的对象存储、CNI、节点内核差异很大，不能把 ACK 结果直接推广到 GKE/EKS/AKS。
2. gVisor filesystem 配置会显著影响结果，必须在报告中固定配置。
3. Firecracker/Kata 与 Substrate + gVisor 的隔离边界不同，只能作为工程对照，不能简单按单项指标判优。
4. actor workload 如果过于 synthetic，可能低估 fork/exec、小文件和外部 API 对真实 agent 的影响。
5. P99/P999 对控制面抖动敏感，测试期间必须记录集群后台任务、节点重启、镜像拉取和对象存储错误。
6. snapshot size 是恢复延迟的核心解释变量，所有 restore 结果必须按 snapshot size 分桶。
7. 对象存储应区分内网 endpoint 和公网 endpoint，否则结果不可解释。
8. 每次测试前必须确认 `/dev/kvm`，否则 gVisor KVM 和 systrap 结果会混淆。
9. 如果一轮测试暴露 instrumentation 缺口，应先补观测再重跑，而不是扩大规模。
10. 多轮测试之间只能比较 blessed run；partial、aborted、contaminated run 不能进入趋势图。

## 最终报告模板

最终性能报告必须以 HTML 形式交付，入口为 `reports/index.html`。报告内容建议包含：

1. Executive summary。
2. Environment and versions。
3. Runtime baseline comparison。
4. Substrate lifecycle latency。
5. Snapshot locality analysis。
6. Density frontier。
7. 10/50/100 node scale trends。
8. Sandbox scale comparison。
9. Correlation and linked-trend analysis。
10. API-driven scale-out impact。
11. Burst behavior。
12. Soak and failure recovery。
13. Bottleneck attribution。
14. Multi-round learning log。
15. Regression gate。
16. Cost and resource usage。
17. Recommendations for Substrate community changes。
18. Chart appendix。
19. Raw data index。

每个结论必须附带：

- 测试 run-id。
- 样本数。
- P50/P95/P99。
- error rate。
- 资源曲线截图或 CSV。
- 对应图表 ID。
- 对应日志或 trace 链接。
- run 状态：blessed、partial、aborted 或 contaminated。

最终报告至少要包含这些 dashboard 页面：

- `overview-scale-trends`: 10/50/100 节点的 latency、throughput、error、cost 总览。
- `sandbox-comparison`: runc、gVisor systrap、gVisor KVM、Kata、Firecracker-ref 的分维度对比。
- `substrate-lifecycle`: create/pause/resume/commit 的 stage breakdown。
- `snapshot-locality`: local hit rate、snapshot size、OSS latency、restore latency 联动。
- `control-plane`: ateapi、Valkey、worker cache、Kubernetes API 联动。
- `scale-out`: API 扩容事件、node readiness、worker readiness、P99/error rate 时间线。
- `soak`: 12/24 小时 latency drift、RSS、disk、snapshot object count。

## Substrate 可观测性前置项

在正式大规模测试前，必须补齐或明确替代采集方式，否则结果很难归因：

1. actor lifecycle stage latency histogram。
2. worker assignment reason and latency。
3. local snapshot hit/miss reason。
4. checkpoint/restore per-stage latency。
5. object storage operation latency/error。
6. Valkey operation latency/error。
7. worker cache age and relist count。
8. runsc checkpoint/restore exit code and duration。
9. no free workers 的细分原因。
10. locality conflict count。

这些指标与 ACK 优化报告中的 worker locality、Valkey、snapshot fallback、observability 建议一致，应作为性能测试前置工作。若短期不能补齐，需要在 run matrix 中显式标记缺失项，并把该 run 降级为 exploratory。
