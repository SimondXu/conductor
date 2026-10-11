# Conductor — Plan of Record

> Working document. Records the current upstream baseline, the verified gaps,
> the agreed scope, and the implementation order. Update it when a decision
> changes; do not let it drift.

**Conductor** — a vendor-neutral, crash-recoverable fork of Google's AX with
plain-Kubernetes execution, durable tool effects, and first-class operational
telemetry.

Run locally or on standard Kubernetes without requiring Agent Substrate or
Google Cloud services.

---

## 0. Baseline

| | |
|---|---|
| Upstream | `github.com/google/ax` (Apache-2.0) |
| Pinned commit | `b777313` — *Add doctor command to diagnose sidecar, assets, and runtime* (2026-08-19) |
| Policy | Pin and diverge. Do **not** track `main`. Cherry-pick only when a specific upstream change is useful. |
| Upstream PRs | Externally paused during architectural redesign. Upstreaming is upside, never a dependency. |
| Attribution | Apache-2.0 requires retaining notices and stating modifications. README must name the fork and list what Conductor adds. Never present upstream subsystems as originally authored here. |

Upstream size at the pin (non-test Go):

| Package | LOC |
|---|---|
| `internal/harness` | 4,267 |
| `proto` (incl. generated) | 3,362 |
| `cmd` | 2,870 |
| `internal/controller` (incl. `eventlog`) | 740 |
| `internal/skills` | 1,581 |
| `internal/pythonsidecar` | 963 |
| `internal/config` | 796 |
| `internal/server` | 206 |
| `internal/ate` | 109 |
| `internal/telemetry` | 106 |
| Total Go | 16,311 |

The durable-execution core that Conductor targets is small: **~950 LOC**
(`internal/controller` + `internal/server`).

---

## 1. Current architecture (verified against the pinned commit)

```
Client (ax CLI / gRPC)
   │  InteractionsService.CreateInteraction(CreateInteractionEvent)
   │  → stream CreateInteractionResponse
   ▼
internal/server            server.go — gRPC server
   │                       inFlight map[string]struct{} + sync.Mutex   (server.go:43)
   │                       otelgrpc.NewServerHandler()                 (server.go:84)
   ▼
internal/controller        controller.go — Controller.Exec
   │  ├─ logger.ResumptionState()  → (state, harnessID)   (controller.go:224)
   │  ├─ registry.go               → resolve Harness
   │  └─ logger.LogInputs/LogOutputs → eventlog.Append
   ▼
internal/harness           harness.go — Harness / Execution / Handler
   ├─ antigravity/            thin gRPC client → Python sidecar (Gemini)
   ├─ antigravityinteractions/ direct HTTPS/SSE → Vertex Interactions API,
   │                           executes tools in-process
   └─ substrate/              remote gVisor actor via Agent Substrate
                              Start: CreateActor → ResumeActor → dial worker
                                     → waitForHealthy   (substrate.go:87-133)
                              Run:   send turn          (substrate.go:194)
                              Close: SuspendActor       (substrate.go:235-256)
   ▼
internal/controller/eventlog
   sql.go / sqlite.go / postgres.go
   ▼
PostgreSQL or SQLite
```

### Event log schema — the entire persistence layer

Identical DDL in `sqlite.go:44-49` and `postgres.go:45-50`. This is the **only**
`CREATE TABLE` in the repository.

```sql
CREATE TABLE IF NOT EXISTS conversation_log (
    conversation_id TEXT    NOT NULL,
    step            INTEGER NOT NULL,
    payload         TEXT    NOT NULL,   -- protojson-encoded StepEvent
    PRIMARY KEY (conversation_id, step)
);
```

### Protocol surface

`ExecutionService` / `ExecRequest` / `last_seq` were **removed** upstream in
`2282424` (2026-08-11). Current surface:

```protobuf
message CreateInteractionEvent {
  string conversation_id = 1;
  repeated Step inputs   = 2;
  reserved 3;                 // ← last_seq used to live here
  string agent_id        = 4;
  bytes  agent_config    = 5;
}

service InteractionsService {
  rpc CreateInteraction(CreateInteractionEvent)
      returns (stream CreateInteractionResponse);
}

// TODO(jbd): CreateInteraction should return an Interaction message
// and the outputs should be polled from the Interaction.
```

`Step.type` oneof: `ContentStep` | `ThoughtStep` | `ToolCallStep` | `ToolResultStep`.
`enum State`: `UNSPECIFIED | PENDING | FAILED | COMPLETED | CANCELED` — turn-level
only; there is no per-tool-call lifecycle enum, no `WAITING_FOR_APPROVAL`.

### Harness contract — note the documented invariant

`internal/harness/harness.go`, on the `Harness` interface:

> Single-writer expectation: the controller must ensure that at most one
> Execution exists per conversation id at a time. Harness implementations rely
> on this invariant — for example, a harness that durably persists
> per-conversation state may use a last-write-wins store without
> compare-and-swap, which is correct only because there is a single writer per
> conversation.

This matters: see gap G4. Upstream **documents** that harnesses depend on
single-writer, while single-writer is enforced only by an in-process mutex.

---

## 2. Verified gaps

Every claim below was read in the source at the pinned commit.

### G1 — Tool calls and results never reach the event log  *(highest value)*

In `antigravityinteractions`, `Handler.OnMessage` is invoked from exactly one
place — `emitText()`, carrying `res.modelText`. The tool loop:

```go
out := e.harness.executeTool(ctx, call)     // external side effect happens here
next = append(next, toolResultStep{...})    // in-memory slice only, never emitted
```

Tool calls and tool results are never handed to the controller, therefore never
appended to `conversation_log`. **The event log contains model text only.**

Consequence: replay-based recovery is not merely unimplemented, it is
*structurally impossible* — you cannot rebuild execution from a log that does
not record what the agent did.

Note the `Handler`/`Step` interfaces already support this (`Step.type` includes
`ToolCallStep` and `ToolResultStep`). The gap is in the harness implementation,
not the protocol. The fix is small and clean.

Not present in the upstream issue tracker.

### G2 — Durable cursor advances before the side effect, and lives on local disk

`antigravityinteractions.Run()` order per turn:

```
postTurn()        → model response + tool calls   (turn committed server-side, Store: true)
prevID = res.interactionID
setPrevID(prevID) → DURABLE WRITE (fsync+rename to a local file)
  ── crash window ──
executeTool(call) → external side effect
postTurn(next)    → results returned to the model
```

Two problems:

1. The cursor advances to the turn *containing* the tool calls before any tool
   runs. Crash in the window and the pending tool calls exist only in the remote
   Interactions service; Conductor never re-fetches them. On resume, `Run()`
   fails with `Run called with no queued input and no work pending`.
2. The only durable local state is a **file** written by `cursorStore`
   (`StateDir`), not the event log or the DB. Another replica cannot read it.

Not present in the upstream issue tracker.

### G3 — Controller does not reconstruct execution state

`internal/controller/controller.go:72`

```go
// TODO(jbd): Resume an incomplete execution if there exists one.
```

The `STATE_PENDING` path calls `h.Start(ctx, conversationID, agentConfig)` then
`exec.Run(...)` with **no intervening `exec.Queue(...)`**.

`ResumptionState()` (`controller.go:224-241`) iterates every event only to pick
the last non-empty `state` and the first non-empty `AgentId`; it discards
`ev.Steps` entirely.

Upstream: [#190](https://github.com/google/ax/issues/190) (open),
[#229](https://github.com/google/ax/issues/229) (open).

### G4 — Single-writer is process-local only

`internal/server/server.go:43-44`

```go
inFlight   map[string]struct{}
inFlightMu sync.Mutex
```

No lease, fencing token, generation, or ownership column exists anywhere in the
repo. Two server replicas can both believe they own a conversation — which
breaks the invariant the `Harness` interface explicitly documents (§1).

Upstream: [#125](https://github.com/google/ax/issues/125) *Validate the single
writer enforcement* (open).

### G5 — No tool-execution ledger

`ToolCallStep` / `ToolResultStep` carry `id`, `name`, `arguments`, `result`,
`is_error`. There is no per-call lifecycle state, no idempotency key, no attempt
counter, and no representation of an uncertain outcome.

### G6 — Full-history replay only

`eventlog/sql.go:73`

```go
"SELECT payload FROM conversation_log WHERE conversation_id = $1 ORDER BY step"
```

No `LIMIT`, no cursor, no snapshot table, no compaction. Replay cost grows with
history length.

Upstream: [#6](https://github.com/google/ax/issues/6) *Introduce ax snapshot* (open).

### G7 — Observability is trace-only and coarse

**Correction to an earlier assessment:** the OTLP endpoint is *not* hardcoded.
`cmd/ax/serve.go:65-76` reads `cfg.Telemetry.OTLP.Endpoint` and falls back to
`WithInsecure()` for non-GCP endpoints; the config comment names
`localhost:4317` as an example. Traces already work against any OTLP collector.

The real gap:

| | |
|---|---|
| OTel tracing | present — but only **3** hand-written spans repo-wide (`eventlog/sql.go:106`, `substrate.go:195`, `antigravity.go:163`) plus `otelgrpc` auto-instrumentation |
| Configurable OTLP endpoint | present |
| Structured logs | partial (`slog` JSON) |
| **Metrics** | **absent** — zero `Meter()` calls; `otel/metric` is an indirect dependency only |
| **Prometheus / dashboards** | **absent** |
| **Recovery / SLO metrics** | **absent** |

Uninstrumented: `postTurn` (the actual model call), `executeTool`, retries
(`fmt.Fprintf(os.Stderr, ...)` only).

Upstream: [#13](https://github.com/google/ax/issues/13) (open).

### G8 — No compute path without Agent Substrate

Distributed execution goes through `internal/ate` → `agent-substrate/substrate`
(external module depending on `client-go`, `apimachinery`, `controller-runtime`).
There is no `Executor` abstraction and no local/Docker/plain-Pod backend
(`grep -ri "docker\|isolation"` over `internal/` and `cmd/`: zero hits).

`manifests/README.md:5`:

> This path is experimental and incomplete: the manifests, scripts, and …

### G9 — Model path: architecture is agnostic, built-ins are not

AX's architecture *is* harness/model agnostic — `HarnessService` is the
documented extension boundary. What is missing is a usable **non-Google
reference harness**. All three built-ins require Gemini/Vertex/Antigravity or
Agent Substrate. There is no OpenAI-compatible path.

State the gap as "no non-Google reference harness", **not** "the runtime is
bound to Gemini". The latter is inaccurate and easy to refute.

### G10 — HITL exists in the proto, not in the runtime

`ConfirmationContent` / `ApprovalDecision` / `DeclineDecision` exist in
`proto/content.proto:28-45`, but there is no `WAITING_FOR_APPROVAL` state in the
`State` enum and the only consumer is the interactive CLI loop in
`cmd/ax/exec.go:202-284`, which blocks in-process. If the CLI dies the pending
approval is lost. Out of scope for Conductor.

### Dead ends — do not plan work here

| Item | Why |
|---|---|
| `last_seq` replay (#320) | Closed 2026-08-13: *"We are closing this issue and will come back to it after the protocol revamp."* The field is gone; `CreateInteractionEvent` has `reserved 3`. |
| Postgres dashboard parity (#307) | The dashboard was deleted upstream in `c0d8e5b` (2026-07-22). The gap no longer exists. |
| DurableDir (#268) | Lives in `agent-substrate/substrate`, a separate repository. Not actionable in an AX fork. |

---

## 3. Scope

Six blocks. Each has a Definition of Done. **If it cannot be written into a DoD,
it is not in scope.**

### ① Bootstrappable

Deterministic `FakeHarness` implementing `harness.Harness` / `harness.Execution`
with programmable actions (think, tool call, delay, fail-once, crash-after-effect,
duplicate callback). One-command local stack.

**DoD** — `git clone && docker compose up` brings up PostgreSQL + Conductor +
FakeHarness + Prometheus + Grafana; a full execution runs end to end with **zero
API keys**; the same path runs in CI.

*Rationale: without a deterministic harness, block ⑥ is impossible — real model
calls are nondeterministic, rate-limited, and cost money. Every resume number
originates here.*

### ② Complete effect log + recovery

- Emit `ToolCallStep` / `ToolResultStep` through `Handler.OnMessage` so tool
  lifecycle reaches the event log (G1).
- Durable tool ledger: stable `call_id`, argument-hash idempotency key, attempt
  counter, terminal states including `OUTCOME_UNKNOWN` (G5).
- Rebuild execution state from the log and `Queue()` it before `Run()` on
  `STATE_PENDING` (G3).
- Close the cursor/effect ordering window (G2).

Effect semantics under partial failure:

```
TOOL_PLANNED  → stable call_id + idempotency_key persisted
TOOL_STARTED  → persisted
              → external side effect
TOOL_SUCCEEDED / TOOL_FAILED

crash between TOOL_STARTED and a terminal state
              → OUTCOME_UNKNOWN

  idempotent / read-only      → retry with the same idempotency key
  reconcilable                → query the external system, then resolve
  non-idempotent, unqueryable → recovery policy / manual resolution
```

**Never claim exactly-once external side effects.** The claim is
*crash-safe, failure-aware tool execution with durable idempotency and
uncertain-outcome reconciliation*.

**DoD** — after any injected crash, the event log alone can reconstruct which
tools were called, with what arguments, what each returned, how many attempts
occurred, and the terminal state of each; the `OUTCOME_UNKNOWN` path has tests.

### ③ Plain-Kubernetes executor

`Executor` abstraction with a `KubernetesPodExecutor` backend requiring **no
Agent Substrate**: create Pod → watch until Ready → connect harness → execute →
SIGTERM drain → cleanup. Plus readiness/liveness, graceful shutdown, PDB,
resource requests/limits, rolling-update behavior.

```
Executor
├── Local            (subprocess)
├── AgentSubstrate   (upstream, kept)
└── KubernetesPod    (ours)
```

**Scope fence:** Pod lifecycle only. **No CRD, no operator, no custom scheduler,
no autoscaler.**

**DoD** — on `kind`, with Agent Substrate absent, deleting an executor Pod lets
the session recover and complete; a rolling update produces no duplicate
terminal state.

### ④ Distributed ownership

PostgreSQL lease + fencing generation (or K8s `Lease`); every append is a
conditional write gated on the current generation. Multiple controller replicas,
single-writer invariant preserved across failover.

**No Raft. No custom consensus.**

**DoD** — three controller replicas; killing the holder transfers ownership; a
write from the stale holder carrying an expired generation is rejected by the
persistence layer.

### ⑤ Operability

Structured logs carrying `conversation_id`, `trace_id`, owner identity, and step
index. Metrics: active sessions, executions total, execution duration, recovery
count / failures / duration, replay count and size, tool attempts, duplicate
effects, controller errors, executor readiness latency. End-to-end spans:
client → server → controller → executor → model → tool → event append →
recovery. Grafana dashboard checked into the repo.

**Sequencing note:** the *logs + a small set of metrics* must land **before**
blocks ③ and ④ — pod-level failure recovery is not debuggable without them. Full
tracing and dashboards can follow.

**DoD** — one trace spans the full path; a Grafana panel answers "how long did
the last recovery take, and how many tool attempts did it involve".

### ⑥ Chaos + load benchmark

Injected failures: client disconnect, controller process/pod kill, executor pod
kill, duplicate request, slow downstream, model/tool timeout, transient DB
failure, rolling restart.

Measured: max concurrent sessions, throughput, p50/p95/p99 latency, recovery
success rate, p95 recovery latency, event loss, duplicate effects, error rate.

**DoD** — 8 failure classes × N injections; every invariant in §4 asserted, not
merely "did it recover"; reproducible with one command.

---

## 4. Correctness invariants (draft — extract to `docs/correctness.md` at step 2)

After any crash, restart, or ownership transfer:

| | Invariant |
|---|---|
| **I1** | Every `TOOL_STARTED` reaches a terminal state (including `OUTCOME_UNKNOWN`). No dangling calls. |
| **I2** | Replaying an idempotent tool from any crash point does not increase the external side-effect count. |
| **I3** | At most one controller holds a valid lease for a conversation at any instant. |
| **I4** | A write carrying an expired generation is rejected by the persistence layer. |
| **I5** | The event log's `step` sequence is contiguous — no gaps, no reordering. |
| **I6** | Post-recovery execution state ≡ state rebuilt by replaying the log. |

Block ⑥ asserts each of these individually.

---

## 5. Anti-scope-creep rules

1. **Tool ledger: three categories, three policies. No plugin system.**
   Idempotent → retry with the same key. Reconcilable → query the external
   system. Neither → `OUTCOME_UNKNOWN` + policy. Three `switch` arms. No policy
   DSL, no effect-plugin framework, no generic saga engine.
2. **`KubernetesPodExecutor` does Pod lifecycle, not scheduling.** No CRD, no
   operator, no custom scheduler, no autoscaler.
3. **Anything without a DoD does not get built.** This prevents mid-flight
   feature growth rather than cutting half-written work later.

---

## 6. Implementation order (locked)

```
 0  Pin b777313, fork, read the streaming path end to end
    (internal/controller 740 LOC, internal/server 206 LOC)
 1  ①  FakeHarness + docker compose, one command, zero API keys
 2      docs/correctness.md — the six invariants
 3  ②  Tool call/result into the event log; stable call_id, idempotency key,
        attempts, OUTCOME_UNKNOWN
 4  ②  Controller execution-state reconstruction (controller.go:72 / #190)
 5  ⑤a Structured logs + core metrics (enough to debug; dashboards later)
 6  ③  KubernetesPodExecutor on kind, no Agent Substrate
 7  ④  PostgreSQL lease + fencing + multi-replica failover
 8  ⑤b Full-path tracing + Grafana dashboard
 9  ⑥  Chaos + load benchmark → real numbers
── P1 ────────────────────────────────────────────────────────
    OpenAI-compatible reference harness (covers OpenAI / Ollama / vLLM)
    Resumable client stream protocol (clean redesign, not a #320 revival)
    Anthropic adapter · gVisor RuntimeClass · event-log snapshot/compaction (G6)
```

The OpenAI-compatible harness sits in P1 deliberately: blocks ①–⑥ complete and
produce every number using `FakeHarness`. A real model backend is a demo
requirement, not a correctness requirement, and costs about a day.

---

## 7. Explicit non-goals

Public `pkg/` API extraction · multiple model providers · Anthropic adapter (P1) ·
resumable stream protocol (P1) · gVisor (P1) · Raft or custom consensus ·
Firecracker · Kafka · service mesh · multi-cloud · agent memory · RAG · model
training or SFT · voice · multi-agent planning · evaluation framework ·
Kubernetes CRD or operator · durable HITL · frontend work.

Rationale for two of these:

- **`internal/` → `pkg/` extraction.** For a runtime, the extension boundary is
  the gRPC `HarnessService`, not Go imports. `internal/` signals "no public API
  commitment", which is reasonable for a project openly warning about breaking
  changes. The refactor costs package cycles, naming, config ownership, and test
  churn while producing almost no distributed-systems signal.
- **Multiple providers.** One OpenAI-compatible harness covers OpenAI, Ollama,
  and vLLM through a single interface. Building a provider matrix turns the
  runtime into a model gateway, which is a different project.

---

## 8. Positioning

**Tagline**

> Conductor — a vendor-neutral, crash-recoverable fork of Google's AX with
> plain-Kubernetes execution, durable tool effects, and first-class operational
> telemetry.
>
> Run locally or on standard Kubernetes without requiring Agent Substrate or
> Google Cloud services.

Do **not** use "AX, without Google Cloud" — AX is architecturally
harness/model-agnostic and a maintainer can refute the claim in one line.

**Selling points, in order of what a reader perceives first**

1. Runs anywhere. `docker compose up`, zero API keys, any OTLP collector, plain
   Kubernetes.
2. The execution log is actually complete — tool calls and results are recorded,
   so recovery can be reconstructed from the log.
3. Effect semantics under partial failure — durable idempotency keys and
   explicit uncertain-outcome reconciliation.
4. Proven, not claimed — a reproducible chaos and load benchmark ships with the
   repo.

**README attribution block (required)**

> Conductor is a fork of [google/ax](https://github.com/google/ax) (Apache-2.0).
> Upstream provides the distributed harness runtime, the gRPC protocol, the
> event log, and the Agent Substrate integration. Conductor adds: …

---

## 9. Resume bullets (numbers are placeholders until block ⑥ runs)

Structure: correctness/durability → distributed ownership + Kubernetes →
observability + benchmark. Vocabulary drawn from real 2026 postings (Amazon
AgentCore: *"agent execution, session state, tool orchestration … reliability,
and low-latency streaming"*; Anthropic: *"failure, retries, idempotency, and
consistency"*; Moveworks: *"tracing, logging, and metrics frameworks … latency
bottlenecks, race conditions, throughput limitations"*).

**Conductor — Vendor-Neutral Agent Execution Runtime** *(Go, gRPC, PostgreSQL,
Kubernetes, OpenTelemetry)*

- Extended Google's open-source agent runtime into a vendor-neutral distributed
  execution runtime in Go, making the execution log complete and replayable:
  persisted tool-call lifecycle with stable IDs, argument-hash idempotency keys,
  and per-attempt state, then implemented event-sourced recovery so restarted
  controllers resume long-running agent sessions without losing or duplicating
  tool effects — **X% recovery across N runs and 8 injected failure boundaries,
  0 duplicate effects on idempotent tools**.
- Implemented distributed session ownership with PostgreSQL leases and fencing
  tokens, preserving single-writer consistency across M controller replicas
  through pod deletion, failover, and rolling updates; replaced the
  Substrate-only compute path with a plain-Kubernetes executor owning Pod
  lifecycle, readiness, and graceful drain — **session continuity X%, p95
  recovery latency Y ms**.
- Made the runtime operable: extended trace-only telemetry into a full
  observability stack with Prometheus metrics and end-to-end distributed tracing
  across client → controller → executor → model → tool → event log, and shipped
  a reproducible chaos/load benchmark reporting **throughput, p50/p95/p99
  latency, recovery success rate, and duplicate-effect counts at N concurrent
  sessions**.

Never write "exactly-once tool execution".

Portfolio split — the two projects must not overlap:

| ProxyLoop | Conductor |
|---|---|
| Model + Agent | Runtime + Systems |
| Qwen3-8B SFT/QLoRA, teacher-generated trajectories, held-out and false-completion eval, Fast/Slow agent, voice, Temporal workflows | Go, distributed systems, Kubernetes, reliability, concurrency, observability, production operations |
| 58% → 67% completion, 6% → 2% false completion | recovery rate, duplicate effects, p95/p99, throughput |

Do not add Go/Kubernetes to ProxyLoop, and do not add model/agent features to
Conductor.

---

## 10. Open risks to retire in week one

1. **Does Agent Substrate run on `kind`?** If not, block ③ deploys the
   controller plus a plain-Pod harness and skips Substrate entirely. Either
   answer is fine — discovering it in week five is not.
2. **Upstream protocol revamp.** `CreateInteraction` is mid-redesign (see the
   `TODO(jbd)` in `proto/ax.proto`, and #320's deferral). The pin insulates us;
   do not resync opportunistically.
3. **Benchmark honesty.** Every number in §9 must come from a reproducible run
   of block ⑥. No estimates, no extrapolation.
