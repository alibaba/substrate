# Substrate Architecture Atlas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a comprehensive, self-contained Chinese HTML architecture atlas with source-code reading guidance for Agent Substrate.

**Architecture:** A single offline HTML document contains semantic prose, inline SVG diagrams, evidence links, and progressively enhanced interactions. Content is organized around a shared component/status vocabulary so overview diagrams, component drill-downs, lifecycle sequences, roadmap distinctions, and source navigation remain consistent.

**Tech Stack:** HTML5, embedded CSS, vanilla JavaScript, inline SVG, shell-based static checks, Playwright browser verification.

---

## File Structure

- Create `docs/substrate-architecture.html`: the complete offline atlas, including content, inline diagrams, styles, interaction code, and source guide.
- Modify `docs/superpowers/plans/2026-08-03-substrate-architecture-atlas.md`: mark completed steps during execution.
- Do not modify runtime source, APIs, CRDs, manifests, or user-owned unrelated changes.

### Task 1: Establish the content and evidence inventory

**Files:**
- Read: `README.md`
- Read: `docs/architecture.md`
- Read: `docs/glossary.md`
- Read: `docs/api-guide.md`
- Read: `docs/observability.md`
- Read: `docs/threat-model.md`
- Read: `docs/roadmap.md`
- Read: `pkg/proto/ateapipb/ateapi.proto`
- Read: `internal/proto/ateletpb/atelet.proto`
- Read: `internal/proto/ateompb/ateom.proto`
- Read: `pkg/api/v1alpha1/*.go`
- Read: `cmd/*/main.go`
- Read: `manifests/ate-install/*.yaml`

- [ ] **Step 1: Enumerate the core binaries and protocol services**

Run:

```bash
find cmd -mindepth 1 -maxdepth 1 -type d | sort
rg '^service |^  rpc ' pkg/proto/ateapipb/ateapi.proto internal/proto -g '*.proto'
```

Expected: the inventory includes `ateapi`, `atecontroller`, `atelet`, `atenet`, `ateom-gvisor`, `ateom-microvm`, `kubectl-ate`, and `podcertcontroller`, plus the public Control/SessionIdentity and internal AteomHerder/Ateom services.

- [ ] **Step 2: Enumerate declarative resources and deployments**

Run:

```bash
rg '^type (ActorTemplate|WorkerPool|SandboxConfig)' pkg/api/v1alpha1 -g '*.go'
rg '^kind: (Deployment|DaemonSet|StatefulSet|Service|CustomResourceDefinition|ValidatingAdmissionPolicy)' manifests/ate-install -g '*.yaml'
```

Expected: ActorTemplate, WorkerPool, and SandboxConfig appear, together with the control-plane, node, networking, Valkey, and certificate-controller workloads.

- [ ] **Step 3: Resolve implementation versus target status**

Use source/protobuf/manifests as evidence for implemented behavior, current docs plus limitations for evolving behavior, and `docs/roadmap.md` for target behavior. Record no target-only feature as implemented. In particular, show autoscaling, state-store sharding, P2P snapshot sharing, comprehensive actor network policy, and fully actor-correlated telemetry as target/evolving rather than complete.

### Task 2: Build the offline document shell and overview

**Files:**
- Create: `docs/substrate-architecture.html`

- [ ] **Step 1: Create the semantic shell**

Create an HTML5 document with `lang="zh-CN"`, a skip link, sticky `<aside>` navigation, `<main>` sections with IDs `intro`, `overview`, `model`, `control-plane`, `orchestration`, `execution`, `network`, `storage`, `security`, `observability`, `lifecycle`, `maturity`, and `source-guide`, plus a footer source index.

The `<head>` must contain only local metadata and embedded style/script blocks. It must not contain `<link>` stylesheet imports, external scripts, remote images, or analytics.

- [ ] **Step 2: Add the shared visual vocabulary**

Define CSS custom properties for navy surfaces, neutral paper, implemented blue, evolving amber, and target purple. Implement reusable classes `.status-implemented`, `.status-evolving`, `.status-target`, `.component-card`, `.diagram-shell`, `.evidence`, `.protocol`, and `.source-path`.

Each diagram must use the same legend:

```html
<div class="legend" aria-label="架构状态图例">
  <span class="status-implemented">已实现</span>
  <span class="status-evolving">演进中</span>
  <span class="status-target">目标架构</span>
</div>
```

- [ ] **Step 3: Add system positioning and the global overview diagram**

Explain actor-to-worker multiplexing, why Kubernetes remains the infrastructure plane, and why high-frequency actor state bypasses the Kubernetes control plane. Add inline SVG boundaries for clients/frameworks, Kubernetes API/resources, Substrate control plane, networking plane, node execution plane, Valkey, object storage, observability backends, worker pods, and actors.

- [ ] **Step 4: Verify the document is offline and structurally complete**

Run:

```bash
python3 - <<'PY'
from html.parser import HTMLParser
from pathlib import Path

path = Path('docs/substrate-architecture.html')
text = path.read_text()
HTMLParser().feed(text)
assert '<html lang="zh-CN">' in text
assert 'http://' not in text and 'https://' not in text
for section in ('intro', 'overview', 'model', 'control-plane', 'orchestration', 'execution', 'network', 'storage', 'security', 'observability', 'lifecycle', 'maturity', 'source-guide'):
    assert f'id="{section}"' in text, section
print('offline shell OK')
PY
```

Expected: `offline shell OK`.

### Task 3: Add resource models and component decomposition

**Files:**
- Modify: `docs/substrate-architecture.html`

- [ ] **Step 1: Add the resource and dynamic-state model**

Add a relationship diagram and explanation for ActorTemplate, WorkerPool, SandboxConfig, Kubernetes Deployment/Worker Pod, Actor, Worker, Atespace, Golden Snapshot, Last Snapshot, and DurableDir. Explicitly distinguish Kubernetes CRDs from Valkey-backed dynamic records.

- [ ] **Step 2: Add the control-plane decomposition**

Document public gRPC, authentication/interceptors, control API service, lifecycle workflow engine, worker cache/scheduling, Kubernetes informer/template resolution, state-store abstraction, session identity, and credential materialization. Cite `cmd/ateapi/main.go`, `cmd/ateapi/internal/controlapi/`, `cmd/ateapi/internal/store/`, and `pkg/proto/ateapipb/ateapi.proto`.

- [ ] **Step 3: Add the Kubernetes orchestration decomposition**

Document WorkerPool reconciliation into Deployments/worker pods, ActorTemplate golden-snapshot reconciliation, SandboxConfig validation/default resolution, and the role of the Kubernetes API. Cite `cmd/atecontroller/`, `pkg/api/v1alpha1/`, and generated manifests.

- [ ] **Step 4: Add node and sandbox execution decomposition**

Show atelet as a node DaemonSet and AteomHerder server, ateom inside worker pods, sandbox asset fetching, OCI bundle construction, readiness checks, checkpoint/restore, snapshot upload/download, and migration send/receive. Include side-by-side gVisor `runsc` and Kata/Cloud Hypervisor microVM paths.

- [ ] **Step 5: Add network, storage, security, and observability decompositions**

Document atenet DNS, router, Envoy external processing, route lookup/resume, direct proxying, Valkey dynamic state, GCS/S3-compatible snapshot storage, local pause snapshots, pod certificate identity, mTLS, `/run/ate/actor-id`, structured actor logs, OTLP metrics/traces, Prometheus/Jaeger in Kind, and Google Cloud backends in GKE.

- [ ] **Step 6: Check component coverage**

Run:

```bash
for term in ate-api-server atecontroller atelet ateom-gvisor ateom-microvm atenet podcertcontroller kubectl-ate Valkey ActorTemplate WorkerPool SandboxConfig; do
  rg -q "$term" docs/substrate-architecture.html || exit 1
done
echo 'component coverage OK'
```

Expected: `component coverage OK`.

### Task 4: Add lifecycle, communication, maturity, and source guide

**Files:**
- Modify: `docs/substrate-architecture.html`

- [ ] **Step 1: Add the Actor state machine**

Represent suspended, resuming, running, pausing/paused, suspending, migrating, failure/retry, and deleted transitions. Explain that exact intermediate record statuses must be treated according to current protobuf/source definitions and avoid inventing public enum values.

- [ ] **Step 2: Add six communication diagrams**

Add inline SVG sequence diagrams for creation/golden snapshot, request-triggered resume, explicit resume, pause, suspend, and live migration. Each diagram names participating components and annotates DNS, HTTP, public/internal gRPC, Valkey operations, Kubernetes API reads, sandbox calls, and object-storage transfers where applicable.

- [ ] **Step 3: Add the maturity matrix**

Summarize implemented, evolving, and target capabilities by compute, control plane, networking, storage, security, observability, reliability, and operability. Include north-star goals as goals, not measured current performance.

- [ ] **Step 4: Add the source-code reading guide**

Include:

1. a seven-stage reading order from API/CRD definitions through entry points, workflows, node runtime, networking, deployment, and tests;
2. a repository directory map;
3. an entry-point/package table for every core binary;
4. call-chain maps for create, route/resume, pause/suspend, restore/checkpoint storage, and migration;
5. public/internal RPC and responsible implementation tables;
6. a task-oriented "where to change code" table;
7. subsystem test paths and focused test commands.

- [ ] **Step 5: Check protocol and guide coverage**

Run:

```bash
for term in CreateActor ResumeActor SuspendActor PauseActor PrepareActorMigration CommitActorMigration AbortActorMigration GetActorRoute RunWorkload CheckpointWorkload RestoreWorkload ReceiveLiveMigration SendLiveMigration; do
  rg -q "$term" docs/substrate-architecture.html || exit 1
done
for path in 'cmd/ateapi' 'cmd/atecontroller' 'cmd/atelet' 'cmd/atenet' 'cmd/ateom-gvisor' 'cmd/ateom-microvm' 'pkg/proto' 'internal/proto' 'pkg/api/v1alpha1' 'manifests/ate-install'; do
  rg -q "$path" docs/substrate-architecture.html || exit 1
done
echo 'protocol and source-guide coverage OK'
```

Expected: `protocol and source-guide coverage OK`.

### Task 5: Add progressive interactions and accessibility

**Files:**
- Modify: `docs/substrate-architecture.html`

- [ ] **Step 1: Add status filtering**

Add buttons with `data-filter="all|implemented|evolving|target"`. The script toggles `[hidden]` only on elements carrying `data-status`; the default HTML displays all content. Update `aria-pressed` and preserve all content when JavaScript is unavailable.

- [ ] **Step 2: Add navigation and collapsible detail behavior**

Use `IntersectionObserver` to update the active navigation link. Implement disclosure buttons with `aria-expanded` and `aria-controls`, while keeping details expanded in the static HTML until JavaScript adds a `js` class.

- [ ] **Step 3: Add diagram controls**

Implement pointer/wheel zoom and pan on `.diagram-viewport`, reset controls, and fullscreen dialog viewing. Controls must be keyboard reachable, and diagram content must remain horizontally scrollable as the no-script fallback.

- [ ] **Step 4: Add accessibility and print behavior**

Add visible focus styles, reduced-motion rules, semantic `<figure>/<figcaption>`, SVG `<title>/<desc>`, sufficiently large labels, and `@media print` rules that remove navigation/controls and avoid clipping diagrams.

- [ ] **Step 5: Validate JavaScript syntax**

Run:

```bash
python3 - <<'PY'
from pathlib import Path
import re

text = Path('docs/substrate-architecture.html').read_text()
scripts = re.findall(r'<script>(.*?)</script>', text, re.S)
assert len(scripts) == 1
Path('/tmp/substrate-architecture.js').write_text(scripts[0])
print('/tmp/substrate-architecture.js')
PY
node --check /tmp/substrate-architecture.js
```

Expected: Node reports no syntax error.

### Task 6: Browser verification and final evidence audit

**Files:**
- Verify: `docs/substrate-architecture.html`

- [ ] **Step 1: Run the static validation suite**

Run the offline shell, component coverage, protocol/source-guide coverage, placeholder scan, and `git diff --check` commands from earlier tasks.

Placeholder scan:

```bash
rg -n 'TBD|TODO|FIXME|Lorem ipsum|待补充|占位' docs/substrate-architecture.html && exit 1 || true
```

- [ ] **Step 2: Open the file in a real browser**

Use Playwright against the local file or a loopback static server. Verify desktop and mobile viewports, status filters, navigation, disclosures, zoom/reset, fullscreen dialog, keyboard focus, and print layout.

- [ ] **Step 3: Capture and inspect screenshots**

Capture the page overview plus focused screenshots of the global architecture, execution-plane decomposition, request-triggered resume, live migration, and source guide. Confirm no clipped text, overlapping arrows, unreadable labels, or accidental horizontal page overflow.

- [ ] **Step 4: Audit architectural claims**

Cross-check every implemented/evolving claim against the cited source/protobuf/manifest. Confirm roadmap-only items use target styling and north-star numbers are labeled as targets.

- [ ] **Step 5: Commit the completed artifact**

```bash
git add docs/substrate-architecture.html docs/superpowers/plans/2026-08-03-substrate-architecture-atlas.md
git commit -m "docs: add substrate architecture atlas"
```
