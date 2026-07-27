# Substrate 16GiB MicroVM 热迁移可读报告

生成时间：2026-07-26 20:39 CST

## 一句话结论

16GiB microVM 的跨节点热迁移现在已经能做到：用户持续发请求时，迁移从发起到切到目标节点约 7.4 秒，测试中没有观察到 HTTP 失败、POST 丢失、SSE 断流或 WebSocket 断连。

但这不是“绝对 0ms 中断”。更准确的说法是：服务没有失败，连接没有断，用户最大可能感知到的是约 0.8 秒以内的抖动。

## 当前最可用的方案

当前最可用的方案是：

```text
Cloud Hypervisor live migration
+ L7 proxy route switch
+ 强一致 POST proof
+ WebSocket/SSE 连续性探针
```

这条路线的核心不是把 16GiB 内存先完整 checkpoint 到文件再恢复，而是在 VM 运行过程中做 live migration，最后在很短的切换窗口内把用户流量切到目标节点。

实际效果：

| 指标 | 结果 |
|---|---:|
| 从发起迁移到目标节点成功 | 7.391s |
| 底层 Cloud Hypervisor live migration commit | 6.975s |
| HTTP 请求 | 677/677 成功 |
| POST mutation | 677/677 成功 |
| route switch 期间服务错误 | 0 |
| SSE 最大消息间隔 | 757ms |
| WebSocket echo | 673/673 成功 |
| WebSocket 断连 | 0 |
| WebSocket 最大延迟 | 792ms |

这组数据说明：用户请求层面没有中断；体感层面会有亚秒级卡顿，但不是请求失败。

## 用户真实感知是什么

如果用户在迁移时持续访问 sandbox，大概会看到这样的效果：

| 用户行为 | 当前测试结果 | 用户体感 |
|---|---|---|
| 页面持续请求状态 | HTTP 677/677 成功 | 页面不会报错 |
| 后台持续写状态 | POST 677/677 成功 | 已提交 mutation 没有观察到丢失 |
| SSE 持续输出状态帧 | 最大 gap 757ms | 可能有不到 1 秒的停顿 |
| WebSocket terminal 持续输入输出 | 无断连、无 missing echo | terminal 不掉线，可能有不到 1 秒卡顿 |

所以，当前可以写“热迁移期间用户请求没有观察到失败”，不能写“用户完全无感”。

## 为什么 checkpoint/restore 不是主路线

最开始追的是 checkpoint/restore：把 VM 停下来，生成 snapshot，传到目标节点，再恢复。

这条路在小内存下能用，但到 16GiB 后不适合做热迁移。

旧对象存储路径的数据：

| 内存 | 恢复耗时 | 结论 |
|---:|---:|---|
| 512MiB | 9.761s | 接近 10s 边界 |
| 1GiB | 19.553s | 已经太慢 |
| 2GiB | 25.203s | 太慢 |
| 16GiB | 150.146s | 不可用 |

根因不是对象存储单点慢，而是 16GiB 场景下 `pages.img` 实际要搬约 17.2GB。这个文件不是足够稀疏的空洞文件，不能靠压缩或跳过空洞解决。

后面切到本地 RAID 后，checkpoint/restore 被压到 30 秒内可行：

| 阶段 | 耗时 |
|---|---:|
| checkpoint | 7.887s - 8.905s |
| 跨节点复制 17.2GB | 14.63s - 17.23s |
| restore | 0.342s - 0.411s |
| 核心路径总计 | 23.811s - 23.946s |

这说明本地 RAID 适合作“30 秒内跨节点恢复”的路线，但它不是热迁移主路线。真正的热迁移要避免用户窗口里完整搬运 snapshot。

## 不同方案的实际效果

| 方案 | 实测效果 | 是否适合当前目标 |
|---|---|---|
| 旧对象存储 checkpoint/restore | 16GiB restore 150s 级 | 不适合 |
| Fluid / Alluxio | 95s - 132s 级或出现 DataLoss | 不适合 |
| DirectFS | 20s 级恢复 | 可参考，但不是无感热迁移 |
| 本地 RAID checkpoint/restore | 约 24s 核心路径 | 适合 30s 恢复，不适合无感热迁移 |
| BeeGFS | 16GiB write+read 最优约 6.23s | IO 能力好，但生产限制大 |
| Lustre + live migration | 约 7s，GET/POST 0 失败 | 可继续验证 |
| JuiceFS + live migration | 基础场景约 7s，强一致样本有 16.974s | 可用，但尾部要继续压 |
| JindoFS + live migration | 约 7s，GET/POST 0 失败 | 用户窗口可用，cleanup/FUSE 风险要处理 |
| Cloud Hypervisor live migration + L7 route switch | 7.391s，最大体感 gap 792ms | 当前主路线 |

## Lustre / JuiceFS / JindoFS 的结果怎么理解

三种后端都跑过基础 L7 热迁移矩阵，覆盖短请求、慢请求、小上传、小下载、混合轻量请求。

汇总结果：

| 指标 | 结果 |
|---|---:|
| 测试组合 | 15/15 通过 |
| GET | 550/550 成功 |
| POST | 717/717 成功 |
| 最大 SSE gap | 785ms |
| 迁移窗口范围 | 6.811s - 7.766s |
| 底层 live migration 范围 | 6.307s - 6.653s |

这说明：在基础用户请求层面，三种后端都能支撑热迁移。差异主要在运维可靠性、cleanup 稳定性和尾部延迟，而不是“能不能迁移成功”。

## BeeGFS 为什么不作为生产主路径

BeeGFS 的纯 IO 性能很好：

| 配置 | 16GiB write+read |
|---|---:|
| 跨 AZ 单流 | 7.240s |
| 跨 AZ 并行分段 | 6.238s |
| 单 AZ 单流 | 7.042s |
| 单 AZ 并行分段 | 6.232s |

如果只看 IO，它证明“分布式并行文件系统能快速搬 16GiB snapshot”这个方向是对的。

但它现在不适合做生产主路径：

| 风险 | 影响 |
|---|---|
| 开源版最多 5 clients | 扩展性受限 |
| RDMA/eRDMA 未验证通过 | 不能证明能吃满更高网络能力 |
| 生产运维复杂度高 | 需要额外维护存储集群 |

所以 BeeGFS 可以作为性能参考，不建议作为当前生产默认方案。

## 已经做过的关键优化

| 优化 | 解决的问题 | 结果 |
|---|---|---|
| 改走 Cloud Hypervisor live migration | 避免用户窗口内完整 checkpoint/restore | 用户窗口压到 7.391s |
| L7 route switch | 避免迁移期间请求打到错误目标 | route switch 期间服务错误 0 |
| 强一致 POST proof | 防止 GET cache 或全局快照掩盖数据问题 | POST mutation 没观察到丢失 |
| WebSocket/SSE 探针 | 验证长连接不是只靠短请求成功 | 无断连，最大 gap 约 0.8s |
| 用户窗口和 cleanup 分开统计 | 防止 cleanup 失败污染热迁移结果 | 可以准确判断用户迁移窗口 |
| 本地 RAID snapshot | 去掉对象存储慢路径 | checkpoint/restore 核心路径到约 24s |
| 4-stream 大文件复制 | 提升 17.2GB `pages.img` 跨节点复制 | 最好约 14.63s |
| worker assignment 清理修复 | DeleteActor 后 worker 不释放 | 删除后 assignment 可释放 |

## 现在的瓶颈在哪里

热迁移主路线的瓶颈：

| 瓶颈 | 当前表现 |
|---|---|
| Cloud Hypervisor live migration commit | 约 7s |
| 用户体感 gap | 约 0.8s |
| dirty memory 下收敛稳定性 | 单样本通过，还缺矩阵 |

checkpoint/restore 路线的瓶颈：

| 瓶颈 | 当前表现 |
|---|---|
| 跨节点移动 17.2GB `pages.img` | 14.63s - 17.23s |
| runsc checkpoint 遍历内存 | 7.185s - 7.896s |
| restore | 0.342s - 0.411s，不是瓶颈 |
| 本地 snapshot storage | 约 0.0002s，不是瓶颈 |

所以，如果目标是热迁移，继续优化方向不是再优化对象存储，而是压 Cloud Hypervisor live migration 的 commit 时间、route switch 行为和脏页收敛。

如果目标是 30 秒恢复，继续优化方向是减少 `pages.img` 搬运时间或减少需要搬的字节数。

## 还不能宣称完成的部分

这些还没有达到正式生产验收：

| 场景 | 当前状态 |
|---|---|
| Notebook/kernel 迁移 | 没有 kernel id、cell id、变量 checksum 数据 |
| 1GiB 上传中迁移 | 只有 64KiB 小上传 smoke |
| 1GiB 下载中迁移 | 只有 1MiB 小下载 smoke |
| Agent tool exactly-once | 没有 execution_count/result_checksum proof |
| 多 actor 并发迁移 | 没有 N=2/5/10 完整矩阵 |
| 故障注入/回滚 | 没有系统化故障注入数据 |
| cleanup 隔离 | 曾出现 cleanup 窗口请求失败，未通过 |
| pathological dirty memory | 未测 |

## 最终可用性判断

| 目标 | 判定 |
|---|---|
| 单 actor 16GiB 跨节点热迁移 | 可用 |
| 迁移期间 HTTP/POST/SSE/WebSocket 连续访问 | 当前探针下可用 |
| 用户完全无感 | 尚不能这么说 |
| 30s 内跨节点恢复 | 本地 RAID checkpoint/restore 有数据支撑 |
| 10s 内 16GiB checkpoint/restore | 当前数据不支持 |
| 生产级完整 live migration | 还需要补 Notebook、大文件、tool exactly-once、多 actor、故障注入和 cleanup 隔离 |

当前最务实的结论：主线应该继续推进 Cloud Hypervisor live migration + L7 route switch。它已经把问题从“几十秒到几分钟恢复”推进到“7 秒迁移窗口、0 请求失败、约 0.8 秒体感抖动”。下一步优化应集中在降低 7 秒 commit 时间和把 0.8 秒体感 gap 继续压低。

## 数据来源

本报告由已归档的数据报告整理而来，不覆盖原始数据。底稿文件：

`results/live-migration-evidence-20260724/live-migration-data-report-20260726203617.zh.md`
