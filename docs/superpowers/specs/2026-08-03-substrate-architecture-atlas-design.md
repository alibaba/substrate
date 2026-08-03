# Substrate Architecture Atlas Design

## Goal

Create a comprehensive Chinese-language architecture atlas for Agent Substrate as a single, self-contained HTML file. The atlas must explain the system to developers and platform engineers, provide diagrams that can be read independently, and distinguish the repository's current implementation from aspirational or roadmap architecture.

The deliverable will be `docs/substrate-architecture.html`.

## Audience and Scope

The primary audience is engineers who need to understand, operate, review, or contribute to Substrate. The atlas covers:

- the problem Substrate solves and its Kubernetes relationship;
- all core binaries and their internal responsibilities;
- Kubernetes CRDs and dynamic control-plane records;
- actor, worker, snapshot, networking, identity, and telemetry flows;
- gVisor and microVM sandbox paths;
- lifecycle operations, including create, golden snapshot, resume, pause, suspend, and live migration;
- current implementation status, known limitations, roadmap architecture, and north-star goals.

The atlas does not enumerate every Go package or function. It does not access a cluster, cloud API, or runtime environment, and it does not include credentials or environment-specific deployment values.

## Information Architecture

The HTML uses a fixed section navigation and a top-down reading path:

1. System positioning and goals
2. Global architecture overview
3. Resource and state model
4. Control plane
5. Kubernetes orchestration plane
6. Node and sandbox execution plane
7. Networking and traffic plane
8. Snapshot and storage plane
9. Security and identity
10. Observability
11. Lifecycle and communication sequences
12. Current implementation and target architecture

The page begins with a concise executive explanation, then moves from system boundaries into component internals, and finally reconnects the components through lifecycle and communication flows.

## Diagram Inventory

The atlas contains the following diagrams:

- one global system architecture diagram;
- one Kubernetes deployment topology diagram;
- one CRD and dynamic-record relationship diagram;
- component decomposition diagrams for `ate-api-server`, `atecontroller`, `atelet`, `ateom` and both sandbox classes, `atenet`, storage, security/identity, and observability;
- one Actor lifecycle state machine;
- sequence diagrams for Actor creation and golden snapshot creation, request-triggered resume, pause, suspend, explicit resume, and live migration;
- one current-versus-target architecture matrix.

Communication edges identify the relevant boundary, such as gRPC, HTTP, DNS, Kubernetes API, Valkey/Redis, object storage, OTLP, or sandbox-runtime calls.

## Implementation Status Semantics

Every architectural claim uses one of three visual statuses:

- **Implemented**: evidenced by current source code, protobufs, manifests, or configuration.
- **Evolving**: partially implemented, recently changing, or explicitly documented with important limitations.
- **Target**: described only by architecture or roadmap documents and not presented as current behavior.

Implemented and evolving claims include nearby source references. Target claims cite `docs/architecture.md`, `docs/roadmap.md`, or another design document and use a dashed visual treatment. Inferences are labeled as such.

## Visual Design

The page uses a restrained technical-blueprint style rather than a marketing layout:

- deep navy navigation and neutral off-white content surfaces;
- blue for implemented behavior;
- amber for evolving behavior;
- dashed purple for target architecture;
- compact cards, explicit system boundaries, consistent arrow conventions, and a reusable legend;
- responsive layout with horizontally scrollable diagram canvases on narrow screens.

All diagrams are inline SVG. Text is real selectable text, not rasterized labels.

## Interaction Design

The atlas is useful without JavaScript. JavaScript progressively adds:

- section navigation and active-section highlighting;
- status filters for all, implemented, evolving, and target content;
- collapsible detail cards;
- diagram zoom, pan, fullscreen viewing, and reset controls;
- a print-friendly mode.

The interaction layer must not hide required content when scripts fail or are disabled.

## Evidence Sources

The content will be derived from the repository, primarily:

- `README.md`, `docs/architecture.md`, `docs/glossary.md`, `docs/api-guide.md`, `docs/observability.md`, `docs/threat-model.md`, and `docs/roadmap.md`;
- binary entry points and internal packages under `cmd/`;
- public and internal protobuf definitions;
- CRD types under `pkg/api/v1alpha1/`;
- deployment resources under `manifests/ate-install/`;
- relevant migration and snapshot design documents where current code has advanced beyond the older overview documentation.

The HTML includes a source index mapping major sections and claims to repository paths.

## Robustness and Accessibility

The artifact is a single offline HTML file with embedded CSS, JavaScript, and SVG. It does not load CDNs, remote fonts, remote images, analytics, or other network resources.

Semantic headings, landmarks, keyboard-focusable controls, sufficient contrast, reduced-motion support, and accessible diagram labels are required. Diagram containers remain readable through scrolling when zoom controls are unavailable.

## Validation

Completion requires:

- HTML parsing and JavaScript syntax checks;
- confirmation that no remote assets or unresolved placeholders remain;
- source-reference and component-inventory checks;
- browser verification of navigation, filters, collapsible sections, diagram controls, printing, and responsive behavior;
- screenshot review of the overview, component decomposition, and sequence diagrams for clipping, overlap, and unreadable labels;
- a final consistency pass against source code, protobufs, manifests, and current documentation.

## Deliverable Boundary

This work adds the architecture atlas and its design/plan documentation. It does not change Substrate runtime code, APIs, manifests, or behavior.
