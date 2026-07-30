# ACK Tokyo Substrate Environment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create and hand off a verified Tokyo ACK environment running the current Substrate checkout on three nested-virtualization-enabled `ecs.c9i.8xlarge` nodes.

**Architecture:** An ACK Pro managed control plane uses Terway ENI networking and a dedicated VPC. Three Alibaba Linux 3 ECS nodes are created with nested virtualization and attached to one workload node pool; images live in same-region ACR, while snapshots use OSS when authorized or an ESSD-backed in-cluster RustFS fallback.

**Tech Stack:** Alibaba Cloud ACK/ECS/VPC/ACR/OSS, Alibaba Linux 3, Kubernetes, containerd, Terway, `aliyun` CLI, `kubectl`, `ko`, RustFS, gVisor, Kata Containers, Cloud Hypervisor, Go E2E tests.

---

### Task 1: Freeze identifiers and verify account prerequisites

**Files:**
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/state.env`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/preflight/`

- [ ] **Step 1: Create the private state directory**

Run:

```bash
install -d -m 0700 /Users/ringtail/.kube/substrate-tokyo-20260730/evidence/preflight
```

Expected: the directory exists with mode `0700` and is outside the repository.

- [ ] **Step 2: Record non-secret fixed identifiers**

Create `state.env` with mode `0600` and these values:

```text
ACK_REGION=ap-northeast-1
ACK_CLUSTER_NAME=substrate-tokyo-20260730
ACK_VPC_NAME=substrate-tokyo-20260730
ACK_NODEPOOL_NAME=substrate-c9i-8x
ACK_INSTANCE_TYPE=ecs.c9i.8xlarge
ACK_NODE_COUNT=3
ACK_IMAGE_TYPE=AliyunLinux3
ACK_KUBE_CONTEXT=substrate-tokyo-20260730
ACK_STATE_DIR=/Users/ringtail/.kube/substrate-tokyo-20260730
```

Expected: no access key, secret, kubeconfig body, or registry password is stored in this file.

- [ ] **Step 3: Verify identity without a proxy**

Load the two credential lines into `ALIBABA_CLOUD_ACCESS_KEY_ID` and
`ALIBABA_CLOUD_ACCESS_KEY_SECRET`, unset all upper- and lower-case HTTP(S)/ALL
proxy variables, set `NO_PROXY=*`, then run:

```bash
aliyun sts GetCallerIdentity --region ap-northeast-1
```

Expected: account `1373160529477805`, RAM user `moyuan`.

- [ ] **Step 4: Verify quota, inventory, and permissions**

Run read-only ECS availability queries for all Tokyo zones, ACK cluster listing,
VPC/vSwitch listing, ACR listing, and OSS bucket listing. Save sanitized JSON
under `evidence/preflight/`.

Expected: `ecs.c9i.8xlarge` is `Available` in at least one Tokyo zone; no existing
cluster named `substrate-tokyo-20260730`; denied OSS listing is recorded as the
RustFS fallback signal rather than a failure.

### Task 2: Provision the dedicated ACK network and control plane

**Files:**
- Modify outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/state.env`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/ack/`

- [ ] **Step 1: Create a dedicated VPC**

Run `aliyun vpc CreateVpc` in `ap-northeast-1` with name
`substrate-tokyo-20260730` and CIDR `10.240.0.0/16`; record the returned `VpcId`.

Expected: VPC state becomes `Available`.

- [ ] **Step 2: Create Tokyo vSwitches**

Select two zones where `ecs.c9i.8xlarge` is available. Create vSwitches with
CIDRs `10.240.0.0/20` and `10.240.16.0/20`, and record both IDs.

Expected: both vSwitches become `Available` in the selected Tokyo zones.

- [ ] **Step 3: Create the ACK Pro managed cluster**

Use the ACK `POST /clusters` API with name `substrate-tokyo-20260730`, region
`ap-northeast-1`, type `ManagedKubernetes`, spec `ack.pro.small`, Kubernetes
release supported by ACK and by the repository's latest/previous-minor policy,
Terway ENI, `ipvs`, service CIDR `172.21.0.0/20`, pod CIDR `10.241.0.0/16`,
Alibaba Linux 3, containerd, public API endpoint, and deletion protection.

Expected: the asynchronous operation succeeds and cluster state becomes
`running`; record cluster, security-group, VPC, endpoint, and operation IDs.

- [ ] **Step 4: Download and isolate kubeconfig**

Use the ACK user-kubeconfig API, write it with mode `0600` under the state
directory, rename its context to `substrate-tokyo-20260730`, and never commit it.

Expected:

```bash
kubectl --kubeconfig "$ACK_KUBECONFIG" --context substrate-tokyo-20260730 get --raw=/readyz
```

returns `ok` with proxies unset.

### Task 3: Create and attach the nested-virtualization worker pool

**Files:**
- Modify outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/state.env`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/nodes/`

- [ ] **Step 1: Resolve Alibaba Linux 3 image and ECS prerequisites**

Resolve the ACK-supported Alibaba Linux 3 image, SSH key-pair requirement,
security group, and system-disk category for the selected zones. Use ESSD with at
least 120 GiB per node.

Expected: the selected image is Alibaba Linux 3 and supports the target instance
type in both zones.

- [ ] **Step 2: Create three pay-as-you-go ECS instances**

Create three `ecs.c9i.8xlarge` instances across the two vSwitches with enhanced
networking and `CpuOptions.NestedVirtualization=enabled`. Apply tags
`Project=substrate`, `Environment=tokyo-20260730`, and `ManagedBy=codex`.

Expected: all three ECS instances become `Running`; `DescribeInstances` reports
the exact type and nested-virtualization CPU option.

- [ ] **Step 3: Create an ACK node pool and attach existing instances**

Create node pool `substrate-c9i-8x` with Alibaba Linux 3/containerd settings and
attach the three ECS instance IDs using the ACK existing-instance API.

Expected: the attach operation succeeds and exactly three intended Kubernetes
nodes become `Ready`.

- [ ] **Step 4: Prove network and OS properties**

Collect node info, instance metadata, CNI pods, routes, allocatable resources,
and a cold image pull on each node.

Expected: Alibaba Linux 3, containerd, Terway, 32 vCPU-class capacity, and the
intended instance ID are proven for each node.

### Task 4: Enable and verify KVM

**Files:**
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/rendered/kvm-loader.yaml`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/kvm/`

- [ ] **Step 1: Deploy the scoped KVM loader DaemonSet**

Render a privileged DaemonSet limited to the three workload nodes. Its init
command checks `vmx`, loads `irqbypass`, `kvm`, and `kvm_intel`, checks
`/dev/kvm`, and sleeps only after reporting readiness.

Expected: one ready pod per workload node and no scheduling onto unrelated nodes.

- [ ] **Step 2: Run the in-pod KVM probe**

Mount `/dev/kvm` into a probe pod on every node and verify the device can be
opened and the KVM API version queried.

Expected: all three probes pass.

- [ ] **Step 3: Label accepted nodes**

Add `ate.dev/sandboxClass=microvm` only to the three nodes whose KVM probe passed.

Expected: label count is three and every labeled node has passing evidence.

### Task 5: Prepare ACR and object storage

**Files:**
- Modify outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/state.env`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/storage/`
- Create outside Git when needed: `/Users/ringtail/.kube/substrate-tokyo-20260730/rendered/rustfs.yaml`

- [ ] **Step 1: Select or create same-region ACR resources**

Use an existing authorized Tokyo ACR instance/namespace when it is clearly
dedicated or create a new namespace named `substrate-tokyo-20260730`. Configure
VPC access or the narrowest required ACL and obtain short-lived push credentials.

Expected: authenticated `/v2/` succeeds and no account-wide ACL is relaxed.

- [ ] **Step 2: Prove push and per-node cold pull**

Push a uniquely tagged probe image, remove only that probe image from node caches
when necessary, and run one pinned pod per node using the pushed digest.

Expected: all three pulls succeed without public proxy use.

- [ ] **Step 3: Probe OSS permission**

List buckets and attempt only a dedicated-prefix CRUD probe in an authorized
Tokyo bucket. Do not change RAM policy.

Expected: full CRUD selects OSS; any authorization denial selects RustFS.

- [ ] **Step 4: Provide the selected S3-compatible backend**

For OSS, configure its S3-compatible endpoint, path style, and checksum
environment. For fallback, deploy RustFS with an ESSD-backed PVC, create separate
`ate-snapshots` and `kata-assets` buckets, and keep its credentials in Kubernetes
Secrets and the private state directory only.

Expected: PUT, GET, HEAD, LIST, and delete probes pass and the selected backend
is recorded in `state.env`.

### Task 6: Build and deploy Substrate

**Files:**
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/rendered/`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/platform/`

- [ ] **Step 1: Verify the source revision and build baseline**

Record `git rev-parse HEAD`, verify repository Go tests needed by the deployment
path, and set `KO_DOCKER_REPO` to the chosen ACR namespace.

Expected: the revision is recorded and required build tests pass before images
are pushed.

- [ ] **Step 2: Build and push immutable images**

Use `hack/run-tool.sh ko` and repository install scripts to build the platform,
gVisor, microVM, and demo images from the recorded revision. Capture image
digests.

Expected: every deployed image is addressable by digest from all three nodes.

- [ ] **Step 3: Select ACK certificate compatibility mode**

Probe `PodCertificateRequest` and `ClusterTrustBundle`. Use native pod
certificates only when both required APIs work; otherwise render static Secret
certificates and patch only the Substrate components that need them.

Expected: certificate mode is recorded and every component completes its TLS
handshake.

- [ ] **Step 4: Install the platform and verify health**

Apply CRDs, validation policies supported by the cluster, Valkey, ate-api-server,
atecontroller, atelet, atenet, and gVisor `SandboxConfig` using the dedicated
kubeconfig/context and selected bucket.

Expected: CRDs are established, Valkey reports `cluster_state:ok`, deployments
are available, daemon sets are ready on intended nodes, and no platform pod is in
CrashLoopBackOff.

### Task 7: Deploy microVM assets and both demos

**Files:**
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/demos/`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/rendered/counter-microvm.yaml`

- [ ] **Step 1: Assemble and stage the amd64 microVM asset set**

Run `hack/microvm-assets/assemble.sh` for `amd64`, calculate SHA-256 values, and
upload `cloud-hypervisor`, `virtiofsd`, `vmlinux`, `rootfs.img`, and
`configuration-clh.toml` to the selected object backend.

Expected: all five objects are retrievable and match recorded hashes.

- [ ] **Step 2: Deploy gVisor counter demo**

Deploy `demos/counter/counter.yaml.tmpl` through the repository installer using
the chosen ACR and bucket.

Expected: WorkerPool and ActorTemplate become ready.

- [ ] **Step 3: Deploy microVM counter demo**

Render `demos/counter/counter-microvm.yaml.tmpl` with the selected backend and
actual asset hashes, then apply it.

Expected: microVM WorkerPool and ActorTemplate become ready on KVM-labeled nodes.

- [ ] **Step 4: Leave a named handoff actor available**

Create atespace `handoff` and actors `counter-gvisor` and `counter-microvm` from
the two templates.

Expected: both actors can be invoked through `atenet-router` using their Host
headers.

### Task 8: Execute the acceptance matrix and hand off

**Files:**
- Create: `results/ack-tokyo-20260730/acceptance-report.zh.md`
- Create outside Git: `/Users/ringtail/.kube/substrate-tokyo-20260730/evidence/acceptance/`

- [ ] **Step 1: Run automated E2E suites**

Run:

```bash
KUBECTL_CONTEXT=substrate-tokyo-20260730 \
  hack/run-e2e.sh ./internal/e2e/suites/identity -count=1
KUBECTL_CONTEXT=substrate-tokyo-20260730 \
  hack/run-e2e.sh ./internal/e2e/suites/demo -count=1
```

Expected: both commands exit zero with no failed tests.

- [ ] **Step 2: Verify gVisor lifecycle and isolation**

Exercise create/list/get, routed activation, pause/resume, suspend/resume,
full/data snapshot behavior, identity isolation, multiplexing, and delete.

Expected: observed counters and actor states match the design matrix.

- [ ] **Step 3: Verify microVM cross-worker persistence**

Increment the microVM counter, suspend it, prevent reuse of the original worker
for the next resume, resume on another worker, and invoke it again.

Expected: the worker changes and the in-memory counter continues monotonically.

- [ ] **Step 4: Run online/live migration smoke acceptance**

Use the current branch's documented migration workflow for its supported sandbox
class. Capture source/destination workers and request-continuity evidence.

Expected: supported combinations pass; unsupported combinations are marked
`EXCLUDED` with the repository limitation cited.

- [ ] **Step 5: Write and verify the handoff report**

Record timestamps, resource IDs, image digests, kube context, selected object
backend, matrix results, known ACK workarounds, demo commands, and cleanup order.
Scan the report for keys, tokens, passwords, and kubeconfig client data before
committing it.

Expected: every design requirement maps to `PASS`, `FAIL`, or `EXCLUDED`, and no
secret is present.

- [ ] **Step 6: Perform final fresh verification**

Re-run cluster readiness, node/KVM probes, platform rollout checks, demo calls,
the two E2E suites, and report secret scanning.

Expected: all required checks pass immediately before handoff; the cluster and
both named actors remain available for the user.
