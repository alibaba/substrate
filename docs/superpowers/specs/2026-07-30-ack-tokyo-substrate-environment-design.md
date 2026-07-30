# ACK Tokyo Substrate Environment Design

## Objective

Create a new Alibaba Cloud ACK environment in Tokyo for the current Substrate
checkout, deploy both gVisor and microVM workloads, and leave a verified demo
environment running for handoff. The environment must use Alibaba Linux 3,
Terway enhanced networking, and three `ecs.c9i.8xlarge` workers with nested
virtualization enabled.

The environment is for functional acceptance rather than load, capacity, or
long-duration soak testing.

## Safety and Scope

- Region: `ap-northeast-1` only.
- Credentials: read the access key from `~/.kube/chenghao.key`; never print or
  persist it in repository files, command arguments, logs, or generated reports.
- Network access: explicitly unset all proxy variables for Alibaba Cloud,
  Kubernetes, registry, and object-storage operations.
- Billing: use pay-as-you-go resources and three worker instances.
- Existing resources: create a new, clearly named environment. Do not modify or
  delete unrelated VPCs, clusters, registries, buckets, instances, or security
  groups.
- Cleanup: keep the accepted cluster, demo, registry images, and snapshot data
  running for the user. Document cleanup targets, but do not delete them at
  handoff.
- Repository: preserve all pre-existing uncommitted changes. Deployment-specific
  generated files that contain identifiers or credentials stay outside Git.

## Architecture

### ACK and Networking

Create an ACK Pro managed Kubernetes cluster in Tokyo with a public API endpoint
for handoff and a private endpoint for in-VPC traffic. Use Terway ENI networking
and `ipvs` service proxying. Allocate a new VPC and vSwitches with non-overlapping
pod and service CIDRs.

Use a single workload node pool containing three `ecs.c9i.8xlarge` instances.
Each instance provides 32 vCPU and 64 GiB memory. Create the ECS instances with
`CpuOptions.NestedVirtualization=enabled`, then attach them to the ACK node pool
through the supported existing-instance workflow. Configure the node pool for
Alibaba Linux 3, containerd, ESSD system disks, and enhanced networking.

Label the accepted nodes with `ate.dev/sandboxClass=microvm` after KVM readiness
is proven. Do not add the label to a node that lacks `/dev/kvm`.

### KVM Readiness

The ACK image may not load KVM modules automatically. On every worker, verify the
`vmx` CPU flag, then load `irqbypass`, `kvm`, and `kvm_intel` in dependency order.
A privileged, tightly scoped DaemonSet may perform this initialization and expose
readiness evidence. Acceptance requires `/dev/kvm` on all three nodes and a small
KVM access probe from a pod.

### Image Registry

Use Alibaba Cloud ACR as the authoritative image registry. Prefer a same-region
registry endpoint reachable over the VPC. Build Substrate and demo images from
the current checkout using the repository's pinned `ko` workflow, push immutable
commit-qualified tags, and configure Kubernetes image-pull credentials through
the ACK ACR credential helper or a dedicated `imagePullSecret`.

Before deployment, test DNS, TCP/TLS, `/v2/`, authentication, push, and a cold
pull from every node. If an ACR Enterprise ACL is present, restrict changes to the
new environment's egress addresses.

### Object Storage

Treat OSS as an optional preferred backend because the supplied RAM user may not
have OSS permissions. First perform read-only permission discovery and a scoped
bucket or prefix probe. If a usable same-region OSS bucket is available, use it
for Substrate snapshots and microVM runtime assets through the S3-compatible
endpoint with path-style addressing when required. Set the AWS SDK checksum
controls required by OSS:

```text
AWS_REQUEST_CHECKSUM_CALCULATION=when_required
AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
```

Use a dedicated bucket or prefix for this environment. Scope credentials to the
minimum bucket actions needed for object upload, download, listing, and removal.

If OSS discovery or the scoped probe returns an authorization failure, do not
broaden RAM permissions and do not block the deployment. Deploy the repository's
supported RustFS S3-compatible service inside ACK with an ESSD-backed persistent
volume. Use dedicated buckets for snapshots and microVM assets. Keep the storage
service and its data at handoff, and document the later OSS migration settings.
Acceptance records object metadata but does not expose credential values.

### Substrate Deployment

Deploy the current checkout, not an unrelated published build. Install CRDs,
Valkey, ate-api-server, atecontroller, atelet, atenet, the certificate fallback
needed by ACK, and both gVisor and microVM sandbox configurations.

ACK may not expose `PodCertificateRequest` and `ClusterTrustBundle`. Probe the
APIs before installation. If absent, use the repository's known static-secret
certificate workaround and record the deviation. Confirm Valkey reports
`cluster_state:ok` before accepting ate-api-server health.

The gVisor runtime uses its normal `SandboxConfig`. The microVM runtime uses the
repository's Kata/Cloud Hypervisor asset assembly process, stages the resulting
assets to the selected object backend, and pins their SHA-256 values in the
deployed `SandboxConfig`.

## Acceptance Matrix

### Infrastructure

- The ACK cluster is `Running` and its public and private API endpoints respond.
- Exactly three intended workload nodes are `Ready`.
- All workload nodes report `ecs.c9i.8xlarge`, Alibaba Linux 3, containerd, and
  Terway ENI networking.
- All workload nodes expose `vmx` and `/dev/kvm`; the in-pod KVM probe succeeds.
- ACR push, authenticated cold pull, and per-node pull probes succeed.
- PUT, GET, HEAD, LIST, and delete probes succeed against the selected object
  backend. The result identifies whether the backend is OSS or in-cluster RustFS.

### Platform Health

- All expected Substrate CRDs exist.
- All required deployments, daemon sets, and stateful sets become available.
- Valkey reports `cluster_state:ok` and ate-api-server remains ready.
- ActorTemplate golden snapshots become ready for both sandbox classes.
- The router resolves and forwards an actor request using the documented Host
  header format.

### gVisor Functional Acceptance

- Create, get, list, pause, resume, suspend, and delete Actor operations work.
- HTTP routing activates a suspended Actor.
- Full snapshot pause/resume preserves memory and file state.
- Suspend/resume preserves memory and file state through the selected object
  backend.
- Data-only snapshot cases match the repository E2E expectations.
- Two Actors restored from one golden snapshot retain distinct identities.
- Multiple Actors share a smaller worker pool without identity or state leakage.

### microVM Functional Acceptance

- The microVM `SandboxConfig`, WorkerPool, and ActorTemplate become ready.
- A counter Actor boots and handles routed HTTP requests.
- Suspend writes a guest-memory snapshot and transitions the Actor to suspended.
- Resume on a different worker succeeds and the in-memory counter continues.
- Repeated suspend/resume does not leak the golden Actor identity.

### Automated and Migration Checks

- Run the repository `identity` E2E suite.
- Run the repository `demo` E2E suite for the supported gVisor matrix.
- Run the microVM counter acceptance workflow separately because the demo suite
  intentionally skips some gVisor-specific data-snapshot cases on microVM.
- Run a smoke check for the current branch's supported online/live migration
  path. Record unsupported combinations as explicit matrix exclusions rather
  than treating them as passes.

## Failure Handling

Stop expansion when a prerequisite fails. Preserve evidence from ACK operations,
Kubernetes events, workload logs, ACR probes, and object-storage probes. Apply
only scoped compatibility workarounds already required by the target provider.
Do not alter unrelated account-wide policies or relax ACLs broadly.

If `ecs.c9i.8xlarge` becomes unavailable after the inventory probe, stop and
request approval before changing the instance family or size. If one availability
zone fails, another Tokyo zone with confirmed inventory may be used without
changing the architecture.

## Handoff

Deliver:

- ACK cluster, node-pool, VPC, vSwitch, security-group, ACR, and selected
  object-storage identifiers.
- A dedicated kubeconfig context and the commands needed to inspect the cluster.
- Image names and immutable tags or digests.
- Demo namespace, ActorTemplate, atespace, Actor ID, and invocation command.
- A pass/fail acceptance table with timestamps and evidence paths.
- Provider-specific deviations and remaining risks.
- A precise cleanup inventory and deletion order, without performing cleanup.

Do not include access keys, registry passwords, kubeconfig client credentials, or
object-storage secrets in the committed handoff report.
