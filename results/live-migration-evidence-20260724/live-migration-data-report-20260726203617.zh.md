# Substrate 16GiB MicroVM 热迁移数据报告

生成时间：2026-07-26 20:36 CST

本文只保留数据结论：什么场景、什么数据、是否可用、做过哪些优化、不同方案能达到什么效果。

## 最终结论

| 问题 | 数据结论 |
|---|---|
| 16GiB microVM 跨节点热迁移是否可用 | 当前可用，推荐路线是 Cloud Hypervisor live migration + L7 proxy route switch |
| 用户是否能持续访问 | 在当前 HTTP/POST/SSE/WebSocket 探针下，最终用户窗口样本没有观察到请求失败、mutation 丢失、SSE 断流、WebSocket 断连或 echo 丢失 |
| 从发起迁移到 target 成功要多久 | 最优用户窗口样本：7.391s |
| 用户能感知到的最大停顿 | SSE 最大消息间隔 757ms；WebSocket 最大消息间隔/echo latency 792ms |
| 是否等于绝对 0ms 中断 | 不是。当前结论是“当前探针下无观测失败”，不是数学意义 0ms downtime |
| checkpoint/restore 是否满足 30s | 本地 RAID 核心路径可到 23.8s - 23.9s；从发起开始 30s 具备可行性 |
| checkpoint/restore 是否满足 10s | 当前 16GiB 数据移动模型下没有实测支撑 |
| BeeGFS 是否适合作生产主路径 | 不建议。纯存储性能很好，但开源版 5 client 限制和 RDMA 未验证通过 |
| Lustre/JuiceFS/JindoFS 热迁移是否可用 | 基础 L7 smoke 都通过；迁移窗口约 6.8s - 7.8s，GET/POST 0 失败 |
| 是否已经完成完整真实业务验收 | 没有。Notebook、1GiB 上传/下载、tool exactly-once、多 actor、故障注入仍未完成 |

## 热迁移最终用户体感数据

场景：16GiB microVM，跨节点迁移，迁移期间持续 HTTP GET、POST mutation、SSE stream、WebSocket terminal echo。

| 指标 | 数据 |
|---|---:|
| source -> target | `.172.87` -> `.172.222` |
| 跨节点 | 是 |
| 从发起迁移到 target 成功 | 7.391s |
| Cloud Hypervisor commit live migration | 6.975s |
| HTTP 连续请求 | 677/677 成功 |
| POST mutation | 677/677 成功 |
| route switch 后观测请求 | 599/599 成功 |
| route switch 期间 HTTP | 18/18 成功 |
| route switch 期间 POST | 18/18 成功 |
| route switch 期间服务错误 | 0 |
| SSE events | 677 |
| SSE 最大消息间隔 | 757ms |
| WebSocket messages | 1,288 |
| WebSocket echo | 673/673 |
| WebSocket disconnect | 0 |
| WebSocket missing echo | 0 |
| WebSocket 最大消息间隔 | 792ms |
| WebSocket echo latency p95 | 82ms |
| WebSocket echo latency max | 792ms |
| terminal continuity | 通过 |
| terminal UX | 通过 |

结论：这组数据说明，在当前探针下用户请求没有失败，长连接没有断开，强一致 mutation 没有观察到丢失。用户体感主要不是“请求失败”，而是亚秒级抖动。

## 三种共享存储后端的热迁移数据

场景：16GiB microVM，15GiB resident memory，L7 proxy，持续 GET/POST/SSE，迁移期间验证 route 和 mutation proof。

| 后端 | 场景类型 | 迁移到 target 成功 | CH live migration | GET | POST | SSE 最大间隔 | 结论 |
|---|---|---:|---:|---:|---:|---:|---|
| Lustre | 高频短请求 | 7.459s | 6.642s | 75/75 | 74/74 | 538ms | 通过 |
| Lustre | 慢请求 | 7.245s | 6.488s | 26/26 | 66/66 | 412ms | 通过 |
| Lustre | 小上传 | 6.974s | 6.480s | 30/30 | 15/15 | 287ms | 通过 |
| Lustre | 小下载 | 7.216s | 6.599s | 31/31 | 79/79 | 535ms | 通过 |
| Lustre | 混合轻量请求 | 7.766s | 6.563s | 31/31 | 15/15 | 491ms | 通过 |
| JuiceFS | 高频短请求 | 6.915s | 6.486s | 69/69 | 69/69 | 447ms | 通过 |
| JuiceFS | 慢请求 | 7.314s | 6.549s | 28/28 | 68/68 | 391ms | 通过 |
| JuiceFS | 小上传 | 6.953s | 6.530s | 28/28 | 14/14 | 450ms | 通过 |
| JuiceFS | 小下载 | 7.656s | 6.653s | 30/30 | 76/76 | 785ms | 通过 |
| JuiceFS | 混合轻量请求 | 7.457s | 6.629s | 27/27 | 14/14 | 354ms | 通过 |
| JindoFS | 高频短请求 | 6.811s | 6.520s | 67/67 | 67/67 | 513ms | 通过 |
| JindoFS | 慢请求 | 7.309s | 6.542s | 28/28 | 67/67 | 515ms | 通过 |
| JindoFS | 小上传 | 6.914s | 6.502s | 28/28 | 14/14 | 491ms | 通过 |
| JindoFS | 小下载 | 7.080s | 6.307s | 27/27 | 66/66 | 319ms | 通过 |
| JindoFS | 混合轻量请求 | 7.428s | 6.484s | 25/25 | 13/13 | 420ms | 通过 |

汇总：

| 指标 | 数据 |
|---|---:|
| 后端覆盖 | Lustre / JuiceFS / JindoFS |
| 测试组合 | 15/15 通过 |
| GET | 550/550 成功 |
| POST | 717/717 成功 |
| 最大 SSE gap | 785ms |
| 迁移到 target 成功范围 | 6.811s - 7.766s |
| CH live migration 范围 | 6.307s - 6.653s |

结论：Lustre、JuiceFS、JindoFS 在当前基础 L7 热迁移场景下都可用，用户请求层没有观察到失败。三者差异主要体现在尾部 gap 和 cleanup/运维可靠性，不体现在基础用户窗口成功率。

## 更强一致的 15GiB resident memory 数据

场景：15GiB resident memory，持续 GET/POST，POST 使用 idempotency key，返回本次 mutation 自己提交后的 counter/state_version/last_mutation_id，避免用后续全局快照掩盖一致性问题。

| 后端 | 跨节点 | 迁移窗口 | CH live migration | GET | POST | 失败 GET/POST | 最大请求延迟 | SSE events / 最大间隔 | 结论 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| Lustre | `.172.143 -> .172.141` | 6.900s | 6.555s | 69/69 | 35/35 | 0/0 | 387ms | 54 / 397ms | 通过 |
| JuiceFS | `.172.143 -> .172.142` | 16.974s | 16.709s | 170/170 | 85/85 | 0/0 | 439ms | 156 / 229ms | 用户窗口通过，迁移时间偏长 |
| JindoFS | `.172.141 -> .172.143` | 6.939s | 6.534s | 70/70 | 35/35 | 0/0 | 566ms | 52 / 528ms | 用户窗口通过，cleanup 可靠性有问题 |

结论：强一致 POST 证明下，三种后端都没有观察到 GET/POST 失败或 mutation 回退。JuiceFS 这组迁移窗口明显更长，但用户请求仍成功。

## Dirty memory 数据

场景：16GiB actor 持续改写内存后发起热迁移，用于验证 Cloud Hypervisor precopy 在脏页压力下是否还能维持用户窗口。

| Dirty rate | 迁移到 target 成功 | CH live migration | HTTP | 最大完成间隔 | 最大请求延迟 | SSE 最大间隔 | Terminal 最大间隔 | Echo p95 | route switch 后服务错误 | 结论 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 128MiB/s | 8.330s | 8.069s | 88/88 | 729ms | 710ms | 769ms | 796ms | 536ms | 0 | 单样本通过 |
| 512MiB/s | 8.273s | 7.994s | 85/85 | 811ms | 793ms | 784ms | 798ms | 546ms | 0 | 单样本通过 |
| 1024MiB/s | 7.517s | 7.224s | 79/79 | 516ms | 516ms | 832ms | 798ms | 598ms | 0 | 单样本通过 |

结论：这三档单样本都能迁移，且用户窗口没有观察到服务错误。不能把它扩大成完整 dirty memory 矩阵，因为每档只有一个样本，也没有 pathological 档位。

## checkpoint/restore 路线数据

场景：不用 live migration，而是 checkpoint、移动 snapshot、target restore。

### 旧对象存储路径

| 内存 | Cold boot P99 | Suspend/checkpoint P95 | Warm restore P99 | 结论 |
|---:|---:|---:|---:|---|
| 64MiB | 0.469s | 3.753s | 1.000s | 可用 |
| 256MiB | 0.688s | 5.369s | 4.526s | 可用 |
| 512MiB | 0.971s | 7.779s | 9.761s | 接近 10s 边界 |
| 1GiB | 1.569s | 14.566s | 19.553s | 不满足 10s |
| 2GiB | 2.686s | 24.141s | 25.203s | 不满足 10s |
| 16GiB | 19.855s | 64.832s / 170.651s | 150.146s | 不可用 |

结论：旧对象存储 + 压缩路线在 16GiB 下完全不满足热迁移或 30s 恢复目标。

### 本地 RAID 路线

| 阶段 | 数据 |
|---|---:|
| Checkpoint RPC | 7.887s - 8.905s |
| runsc checkpoint | 7.185s - 7.896s |
| 本地 snapshot storage | 约 0.0002s |
| `pages.img` 大小 | 约 17.2GB，实际占用约 16GiB |
| 4-stream 跨节点 copy | 14.63s - 17.23s |
| Restore RPC | 0.342s - 0.411s |
| 核心路径合计 | 23.811s - 23.946s |

结论：本地 RAID 让 checkpoint/restore 路线从不可用变成 30s 内有可行性。瓶颈不是 restore，也不是本地 snapshot storage，而是跨节点移动约 17.2GB `pages.img`，其次是 runsc checkpoint 遍历内存。

## BeeGFS 数据

场景：把 BeeGFS 作为跨节点 snapshot 介质，测 16GiB write + 16GiB read。

| 配置 | 结果 |
|---|---:|
| 20 targets，跨 AZ，单流最优 | 7.240s |
| 20 targets，跨 AZ，2 jobs segmented 最优 | 6.238s |
| 20 targets，单 AZ，单流最优 | 7.042s |
| 20 targets，单 AZ，2 jobs segmented 最优 | 6.232s |
| 单 AZ repeat-stable | 6.40s - 6.55s |
| 最优写吞吐 | 约 5.4GiB/s |
| 最优读吞吐 | 约 5.6GiB/s |

生产判断：

| 项 | 结论 |
|---|---|
| 纯存储性能 | 很好，可以支撑 16GiB 级 snapshot 快速传输 |
| 开源版 client 数限制 | 最多 5 clients，生产扩展性受限 |
| RDMA/eRDMA | 节点能看到 HCA，但 `rping` / verbs 验证失败，没有可用 RDMA 数据 |
| 当前生产主路径 | 不建议 |

结论：BeeGFS 的纯 IO 数据证明“分布式内存/并行文件系统介质”方向是有效的，但 BeeGFS 开源版和 RDMA 验证状态不适合作当前生产主路径。

## 已做优化与效果

| 优化动作 | 解决的问题 | 数据效果 |
|---|---|---|
| 从 checkpoint/restore 切到 Cloud Hypervisor live migration | 避免迁移窗口内完整 checkpoint、传输、restore | 最终用户窗口 7.391s，HTTP/POST/SSE/WebSocket 无观测失败 |
| L7 proxy route switch | 让用户流量在 target ready 后切换，不把底层迁移暴露成连接失败 | route-switched HTTP 18/18，POST 18/18，service error 0 |
| 迁移期间强一致 POST proof | 避免 GET cache 或全局快照掩盖 mutation 问题 | POST 677/677；15GiB resident 三后端 POST 均 0 失败 |
| WebSocket terminal echo probe | 验证长连接不是只靠短请求成功 | echo 673/673，disconnect 0，missing echo 0 |
| skip final destructive cleanup | 把用户窗口和资源清理分开统计，避免 cleanup 失败污染热迁移结果 | 用户窗口样本 exit code 0；summary 后没有 destructive suspend/delete |
| 本地 RAID snapshot | 去掉对象存储压缩/下载慢路径 | 16GiB 核心 checkpoint/restore 路径约 23.8s - 23.9s |
| 4-stream `pages.img` copy | 提升跨节点移动 17.2GB 文件的吞吐 | copy 约 14.63s - 17.23s；2/8 streams 反而更慢 |
| BeeGFS 并行 segmented IO | 验证分布式文件系统可并行搬运大 snapshot | 16GiB write+read 最优约 6.23s |
| worker assignment 删除释放修复 | DeleteActor 后 worker 不释放，影响后续迁移 | fake assignment 删除后为 null，worker version 1 -> 2，actor key 删除 |

## 不同方案能达到的效果

| 方案 | 当前最好数据 | 用户窗口是否可用 | 生产建议 |
|---|---:|---|---|
| 旧对象存储 checkpoint/restore | 16GiB warm restore 150.146s | 不可用 | 淘汰 |
| Fluid / Alluxio snapshot 路线 | Alluxio 95s - 132s 级；Fluid 出现 DataLoss | 不可用 | 不作为主路线 |
| DirectFS checkpoint/restore | 20s 级 cross-node recover | 不适合作无感热迁移 | 可作为恢复路线参考 |
| 本地 RAID checkpoint/restore | 核心路径 23.811s - 23.946s | 可做 30s 恢复，不是无感热迁移 | 作为冷/温恢复路线 |
| BeeGFS snapshot 介质 | 16GiB write+read 6.23s | 纯存储可行，但生产约束大 | 不建议当前生产主路径 |
| Lustre + live migration | 迁移窗口 6.974s - 7.766s | 可用 | 可继续生产化验证 |
| JuiceFS + live migration | 基础矩阵 6.915s - 7.656s；强一致样本 16.974s | 用户窗口可用，尾部需继续压测 | 可继续验证 |
| JindoFS + live migration | 迁移窗口 6.811s - 7.428s | 用户窗口可用，cleanup/FUSE 风险需处理 | 谨慎验证 |
| Cloud Hypervisor live migration + L7 route switch | 最终用户窗口 7.391s，最大体感 gap 792ms | 当前最佳 | 推荐主路线 |

## 当前还不能宣称通过的场景

| 场景 | 当前数据 | 结论 |
|---|---|---|
| Notebook/kernel 内存态 | 无 kernel id、cell id、变量 checksum 数据 | 未验收 |
| 1GiB 上传中迁移 | 只有 64KiB 小上传 smoke | 未验收 |
| 1GiB 下载中迁移 | 只有 1MiB 小下载 smoke | 未验收 |
| Agent tool exactly-once | 无 execution_count/result_checksum proof | 未验收 |
| 多 actor 并发迁移 | 无 N=2/5/10 完整矩阵 | 未验收 |
| 故障注入/回滚 | 无系统化故障注入数据 | 未验收 |
| cleanup 隔离 | cleanup 窗口曾出现请求失败 | 未通过 |
| pathological dirty memory | 未测 | 未验收 |

## 可用性判定

| 层面 | 判定 |
|---|---|
| 单 actor、16GiB、跨节点、L7 热迁移 | 可用 |
| 用户连续 HTTP/POST/SSE/WebSocket 访问 | 当前探针下可用 |
| 强一致轻量 mutation | 当前探针下可用 |
| 低到高 dirty rate 单样本 | 当前单样本可用 |
| 完整生产级无感热迁移 | 尚未完全证明 |
| checkpoint/restore 作为 10s 恢复路线 | 不可用 |
| checkpoint/restore 作为 30s 恢复路线 | 本地 RAID 路线具备可行性 |
| BeeGFS 作为生产主路径 | 不建议 |

最终判定：如果目标是“迁移期间用户请求不中断”，当前最应该推进的是 Cloud Hypervisor live migration + L7 route switch，并继续压缩 7s 级迁移窗口和 800ms 级最大体感 gap。如果目标是“30s 内跨节点恢复”，本地 RAID checkpoint/restore 路线具备实测支撑。如果目标是“10s 内完整 checkpoint/restore 16GiB”，当前数据不支持。

## 数据来源

本报告数据来自已归档的热迁移用户窗口报告、三后端 L7 矩阵、15GiB resident 强一致矩阵、Dirty memory 补测、checkpoint/restore 性能归档、BeeGFS POC 结果和 worker assignment 修复验证。原始文件仍保留在 `results/live-migration-evidence-20260724/`、`results/memory-performance-archive-20260721.md` 和 `/Users/ringtail/workspace/io/beegfs-poc-results.md`，本文不覆盖任何历史报告。
