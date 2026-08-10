# Substrate on Alibaba Cloud ACK Optimization Report

## 背景

本报告基于一次完整的 ACK 东京环境验收结果整理，目标是判断如果要优化 Substrate 社区代码，让 Substrate 在 Alibaba Cloud ACK 上运行得更稳定、更自动化、更少依赖人工 workaround，应该优先做哪些事情。

验收环境关键信息：

- Substrate 版本：社区 `origin/main`，提交 `c1ab0958f85202faf9f87ea66cd98903a9de763b`
- ACK 地域：`ap-northeast-1`，东京
- ACK 集群：`c5242337555544d6ea5f6675e5e639ac3`
- 业务节点池：10 台 `ecs.c9i.8xlarge`
- 单节点规格：32 vCPU，约 64 GiB 内存
- 节点公网访问：已开启，每台 c9i 节点有公网 IP
- Nested virtualization：ECS 创建时使用 `CpuOptions.NestedVirtualization=enabled`
- Substrate 主要 e2e：
  - `identity`: PASS
  - `demo`: PASS
- Snapshot 后端：Alibaba Cloud OSS，S3-compatible endpoint
- 镜像仓库：
  - 主验证路径：Alibaba Cloud ACR
  - 集群内 registry：已验证可接收镜像

## 总体结论

Substrate 可以在 ACK 上跑通，但目前依赖多个运行态 workaround。最值得贡献给社区的是一个完整的 ACK provider profile，把证书、ACR、OSS、KVM、Valkey、worker locality 这些验收中暴露的问题变成安装器和控制面的标准能力。

建议方向不是单点修补，而是把 ACK 当成一个 managed Kubernetes provider 做成一等支持：

- 安装前自动探测集群能力。
- 根据 provider 自动选择证书、registry、object storage、node preflight 策略。
- 对运行态依赖，如 Valkey、KVM、worker cache，提供自愈和可观测性。

## 1. PodCertificate / ClusterTrustBundle 兼容

### ACK 上的问题

社区原生 manifest 依赖 Kubernetes `certificates.k8s.io` 相关能力，包括 `PodCertificateRequest` 和 `ClusterTrustBundle`。ACK 当前没有暴露这些 API，因此原生安装路径不能直接工作。

验收中使用了静态 Secret 证书 workaround 才完成部署。

### 社区已有情况

社区已有直接相关 issue：

- `#104`: `ate-system install hangs on managed K8s when certificates.k8s.io/v1beta1 isn't exposed (GKE alpha is a real-world case)`
- `#105/#114`: 已做过 certificates API 缺失时 graceful skip

现有实现更多是“避免挂死”，不是完整 fallback。

### 其他云或上游情况

Kubernetes 上游已有 PodCertificateRequest / ClusterTrustBundle 机制，但 managed Kubernetes 是否暴露取决于云厂商控制面。GKE alpha 曾遇到类似问题，ACK 当前也属于这个类别。

### 建议贡献

新增证书模式：

- `podcert`
- `static-secret`
- `cert-manager`

安装器逻辑：

1. 探测 `podcertificaterequests.certificates.k8s.io`。
2. 探测 `clustertrustbundles.certificates.k8s.io`。
3. 如果存在，走社区原生 podcert。
4. 如果不存在，自动生成静态证书 Secret。
5. 自动 patch 所有组件挂载和环境变量。
6. 输出明确的 provider compatibility report。

优先级：P0。

## 2. Nested Virtualization 与 KVM 设备

### ACK 上的问题

ECS `ecs.c9i.8xlarge` 可通过 `CpuOptions.NestedVirtualization=enabled` 创建，节点 CPU flags 中可以看到 `vmx`。

但 ACK ContainerOS 默认不会自动加载 KVM 模块，初始状态没有 `/dev/kvm`。验收中需要按以下顺序加载模块：

```text
irqbypass -> kvm -> kvm_intel
```

加载后 `/dev/kvm` 出现，10 台 c9i 节点均验证通过。

### 社区已有情况

未找到 Substrate 社区中针对 ACK KVM module loader 或 nested virtualization preflight 的直接实现。

社区有 pluggable ateom backend、microVM、gVisor 等方向，但没有 provider-specific KVM readiness 处理。

### 其他云或上游情况

GKE Standard 支持通过 node pool 配置启用 nested virtualization。GCE VM 也有 `enableNestedVirtualization` 类能力。

ACK 当前验证到 ECS API 支持 nested，但 ACK 节点池 API 没有明显一等字段；本次通过 ECS 直创 + attach 到 ACK 节点池完成。

### 建议贡献

新增 optional `node-kvm-loader` DaemonSet：

- 只调度到真实 ECS 节点，排除 virtual-kubelet。
- 检查 `vmx` / `svm`。
- 检查 KVM 模块文件。
- 按依赖顺序加载模块。
- 验证 `/dev/kvm`。
- 写 Kubernetes Event 或 Node condition。

`atelet` 启动时增加 preflight：

- 如果 sandbox backend 需要 KVM，但 `/dev/kvm` 不存在，明确报错。
- 错误信息包含建议安装 `node-kvm-loader`。

优先级：P0。

## 3. 私有镜像仓库与认证

### ACK 上的问题

验收中 ACR 暴露了多个问题：

- ACR 企业版公网端点启用了 ACL。
- 新增 c9i 节点公网 IP 不在 ACL 中时，拉镜像表现为 TCP timeout。
- ACL 放通后，如果 `acr-pull` 或本地 Docker/ko 凭据过期，会变成 `401 Unauthorized`。
- 旧节点因为镜像缓存容易掩盖问题，新节点会立即失败。

### 社区已有情况

社区已有直接 issue：

- `#432`: `Feature request: Support 3rd party private container registries + authentication like EKS`

该 issue 指出当前实现偏 GCP ADC / localhost registry replacement，对 EKS/ECR、AKS/ACR、GHCR、Harbor、Artifactory 等支持不足。

### 其他云或上游情况

Kubernetes 通用机制是 `imagePullSecrets`。EKS/ECR 有 IAM 和 pod execution role 路径。ACK/ACR 则通常涉及 ACR token、imagePullSecret，以及企业版公网 ACL 或 VPC endpoint。

### 建议贡献

新增 registry provider abstraction：

```yaml
registry:
  push:
    url: ...
    authSecret: ...
  pull:
    url: ...
    authSecret: ...
  provider: acr | ecr | gcr | generic
```

ACK/ACR provider 应支持：

- 获取 ACR authorization token。
- 创建或刷新 Kubernetes imagePullSecret。
- patch 所有 Substrate ServiceAccount。
- 检查每个节点访问 registry `/v2/`。
- 检查 ACR ACL 是否包含节点出口 IP。
- 输出诊断：DNS、TCP、TLS、auth 分阶段结果。

优先级：P0。

## 4. Valkey Stale IP 与集群自愈

### ACK 上的问题

验收中 Valkey Pod 都 Running，但 `cluster_state:fail`，`ate-api-server` CrashLoop，根因是 Valkey cluster nodes 里持久化了旧 Pod IP。

修复方式是 reset Valkey cluster metadata 并基于 StatefulSet DNS 重新创建 slots。

### 社区已有情况

社区已有直接相关 issue/PR：

- `#225`: `Valkey statefulset needs to be restarted daily`
- `#266`: `update valkey to use hostname`
- `#258/#357`: Valkey operations handbook
- `#425/#426`: Redis multi-master pagination 问题

说明社区已经意识到 Valkey/Redis 层是控制面稳定性的核心。

### 其他云或上游情况

StatefulSet + headless service 是 Kubernetes 上运行 Redis/Valkey cluster 的常见模式，但如果 `nodes.conf` 持久化旧 IP，仍需要明确的 announce hostname 或自动 repair。

### 建议贡献

新增 Valkey health/repair 机制：

- 启动时检测 `cluster_state`。
- 检测 `cluster nodes` 中是否包含当前 Pod CIDR 外或已不存在的旧 IP。
- 自动执行受控 repair：
  - 暂停 ate-api-server 写入。
  - flush/reset 或迁移 slots。
  - 基于 StatefulSet DNS 重新建 cluster。
- `ate-api-server` 对 Valkey down 应持续 retry，而不是长期 CrashLoop。
- 提供 `ate valkey doctor` 命令。

优先级：P0。

## 5. OSS / S3-compatible Snapshot

### ACK 上的问题

Alibaba OSS S3-compatible endpoint 不支持 AWS SDK 默认 trailer checksum 行为。验收中需要设置：

```text
AWS_REQUEST_CHECKSUM_CALCULATION=when_required
AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
```

否则 snapshot 上传会失败。

### 社区已有情况

社区已有直接相关 issue：

- `#362`: `Feature request: Fallback to local snapshot when failing to upload external snapshot`
- `#372`: `Ensure Atelet API is idempotent`

社区也已经有 S3 endpoint customization 相关实现：

- `#180`: `feat(objectstorage): refactor storage package and add S3 endpoint customization`

但还没有 Aliyun OSS preset。

### 其他云或上游情况

AWS SDK 新版本默认启用更严格的 S3 checksum/data integrity 行为。MinIO、OSS、其他 S3-compatible 服务经常需要 provider-specific 配置。

### 建议贡献

新增 object storage provider preset：

- `aws-s3`
- `gcs`
- `aliyun-oss-s3`
- `minio`

`aliyun-oss-s3` 自动配置：

- endpoint
- path-style
- region
- checksum env

snapshot 逻辑优化：

- external upload 失败时保留 local snapshot。
- Actor status 记录 external snapshot failure reason。
- Checkpoint/Restore API 幂等。
- 对 OSS 上传错误做结构化分类。

优先级：P0/P1。

## 6. Worker Assignment / Locality 错误语义

### ACK 上的问题

demo e2e 首次失败：

```text
rpc error: code = FailedPrecondition desc = no free workers available
```

随后重跑通过。日志显示其他 namespace 的 ResumeActor 正常，说明不是全局容量不足，更像 worker cache、local snapshot locality、worker 状态同步或错误语义问题。

### 社区已有情况

社区已有直接 issue/PR：

- `#398`: locality-restricted resume failures report misleading `no free workers available`
- `#401`: explain locality conflicts instead of `no free workers available`
- `#397/#399`: empty node name in `NodeVmsWithLocalSnapshots` 导致 actor 不可调度
- `#337`: WorkerCache periodic relist，已关闭
- `#390`: worker cache metrics，已关闭
- `#347`: workercache and valkey store metrics，仍开放

### 其他云或上游情况

这是 Substrate 自身调度/状态模型问题，不是 ACK 特有。但 ACK 节点多、Pod 调度快、节点 IP 变化多时更容易触发。

### 建议贡献

把 worker assignment failure reason 结构化：

- `NO_FREE_WORKER`
- `LOCALITY_CONFLICT`
- `WORKER_CACHE_STALE`
- `WORKER_DELETING`
- `SNAPSHOT_NODE_MISSING`
- `SANDBOX_CLASS_MISMATCH`

API 返回和日志都使用结构化原因。e2e 中在 resume 前增加 worker cache readiness gate。

优先级：P1。

## 7. Worker Cache 与调度可观测性

### ACK 上的问题

规模化到 10 台 c9i 后，worker 数量和调度事件明显增加。没有足够指标时，很难区分：

- worker 未 Ready
- worker cache 未同步
- Valkey stale record
- locality conflict
- 镜像未拉取
- sandbox preflight 未通过

### 社区已有情况

社区已有部分实现：

- `#337`: WorkerCache periodically relist workers
- `#390`: worker cache metrics and store publish-failure counter
- `#347`: instrument both workercache and valkey store with metrics

### 建议贡献

增加 ACK 验收需要的 metrics：

- worker cache size by namespace/pool
- free worker count
- assigned worker count
- assignment failure by reason
- snapshot locality conflict count
- Valkey operation latency/error
- image pull readiness by node
- KVM readiness by node

优先级：P1。

## 8. ACK Provider Profile

### 当前缺口

没有找到社区已有 ACK profile、Aliyun OSS preset、ACR adapter、ACK KVM loader 的直接实现。

其他云已有零散能力：

- GKE 有 nested virtualization node pool。
- EKS/ECR 有私有镜像认证路径。
- Kubernetes 有 imagePullSecrets。
- AWS SDK 有 S3 checksum 配置。

但 Substrate 还没有把这些抽象成 provider profile。

### 建议形态

新增安装入口：

```bash
hack/install-ate.sh --profile=ack \
  --registry-provider=acr \
  --storage-provider=aliyun-oss-s3 \
  --cert-mode=auto \
  --enable-kvm-loader \
  --enable-registry-preflight \
  --enable-valkey-repair
```

profile 自动处理：

- certificates API 探测。
- static Secret fallback。
- ACR pull secret 创建/刷新。
- ACR ACL/节点出口诊断。
- OSS checksum 配置。
- KVM loader 安装。
- virtual-kubelet 排除。
- Valkey health check。
- e2e 前置检查。

优先级：P0。

## 建议开源贡献顺序

### P0

1. `provider/ack: add installation profile`
2. `cert: add static-secret fallback for managed Kubernetes`
3. `storage: add aliyun-oss-s3 preset`
4. `registry: add pluggable private registry auth and ACR adapter`
5. `node: add KVM preflight and optional loader DaemonSet`
6. `valkey: add stale-IP detection and repair`

### P1

1. `scheduler: structured worker assignment failure reasons`
2. `atelet: make checkpoint/restore idempotent`
3. `snapshot: fallback to local snapshot when external upload fails`
4. `observability: ACK dashboard and worker/Valkey metrics`

### P2

1. `e2e: add ACK lane`
2. `registry: automatic ACR ACL sync`
3. `install: provider compatibility report`
4. `docs: ACK production guide`

## 建议 ACK e2e 矩阵

ACK 专用 e2e lane 应覆盖：

- install with static cert fallback
- ACR private registry pull
- OSS snapshot upload and restore
- c9i nested virtualization
- KVM device readiness
- Valkey pod restart / stale IP repair
- worker locality resume
- identity e2e
- demo e2e
- in-cluster registry push/pull

验收不应只看 Pod Running，应至少验证：

- 10 台 `ecs.c9i.8xlarge` Ready。
- 每台 c9i 节点 `/dev/kvm` 存在。
- `atelet` 覆盖所有业务节点。
- Valkey `cluster_state:ok`。
- Substrate control plane 所有组件 Running。
- OSS snapshot objects 存在。
- ACR pull 成功。
- identity/demo e2e PASS。

## 风险与注意事项

- ACR 企业版公网 ACL 是实际生产风险点。新增节点公网 IP 如果没有加入 ACL，会表现为镜像拉取 timeout，而不是明确的 authorization 错误。
- 静态证书 fallback 需要考虑证书轮转，否则长期运行会有过期风险。
- KVM loader 需要 privileged DaemonSet，应明确安全边界和 nodeSelector。
- Valkey repair 涉及控制面状态，必须设计成显式、可审计、可回滚。
- Snapshot fallback 不能默默降级，必须反映到 Actor status。

## 最终判断

ACK 上的问题大多不是 Substrate 核心模型不能运行，而是 provider integration 不完整。社区已经有多个问题的局部 issue/PR，但缺少把 managed Kubernetes、私有 registry、S3-compatible storage、KVM readiness、Valkey self-heal 组合起来的一等 provider 支持。

最有价值的贡献是 ACK profile，并把本次验收中的 workaround 固化为自动探测、自动配置和可观测能力。
