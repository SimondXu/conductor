# Conductor — Streaming Path Walkthrough (M0)

> Read-only map of the upstream streaming path at the pin (`b777313`, module
> renamed in `feffa7e`). Every `file:line` refers to commit `7e0e10c`
> (`git show 7e0e10c:<path>`); later commits shift them. §4 has since been
> done: `stream.go`'s `DrainStream` and the duplicated turn body now live in
> `internal/harness/remote` (`remote.Turn`).
> This file describes what the code **does at that commit**; target design lives in
> `ARCHITECTURE.md`, invariants in `correctness.md`. §6 lists the edit points
> for later milestones.

Scope: `internal/server` (`server.go`, `interceptors.go`), `internal/controller`
(`controller.go`, `registry.go`, `validation.go`, `eventlog/`), the harness
contract (`internal/harness/harness.go`, `stream.go`), the three harnesses,
and the wiring in `cmd/ax`.

---

## 1. Call chain: `CreateInteraction` → harness → event log

### 1.1 Wiring (process start)

| # | What | Where |
|---|---|---|
| W1 | `ax serve` loads config, optionally sets up the OTLP trace exporter | `cmd/ax/serve.go:54-87` |
| W2 | Builds the controller: registry + harnesses + event log builder | `cmd/ax/serve.go:89` → `cmd/ax/internal/cliutil/cliutil.go:54` |
| W3 | `AX_SUBSTRATE=1` picks substrate actors vs. local harnesses | `cliutil.go:59` |
| W4 | `antigravity` registered: local sidecar (`antigravity.New(..., autoStart=true)`) or `substrate.New(...)` | `cliutil.go:97-125` |
| W5 | `antigravity-interactions` registered: in-process `antigravityinteractions.New` or `substrate.New` | `cliutil.go:131-170` |
| W6 | Custom `registry.substrate[]` entries → `substrate.New` | `cliutil.go:175-186`, `internal/config/config.go:262-277` |
| W7 | Default harness falls back to `antigravity` | `cliutil.go:189-196` |
| W8 | Event log: Postgres if `postgres.dsn` set, else SQLite (default `eventlog/log.sqlite`, `config.go:312-314`) | `cliutil.go:200-209` |
| W9 | Event log opened eagerly in `controller.New`; DDL is `CREATE TABLE IF NOT EXISTS` at open | `controller.go:53`, `eventlog/sqlite.go:43-52`, `eventlog/postgres.go:44-53` |
| W10 | `server.New(c)`; `Serve` installs logging interceptors + `otelgrpc` server handler, registers health `"AX"` | `cmd/ax/serve.go:96-108`, `server.go:75-99` |

`ax exec` **without** `--server` builds the same controller in-process
(`cmd/ax/exec.go:112-126`) and calls `Controller.Exec` directly
(`exec.go:344`), bypassing `internal/server` — so it has **no `inFlight`
guard** at all. With `--server` it is a plain gRPC client (`exec.go:359-368`).

### 1.2 Request path

```
client ──CreateInteraction(CreateInteractionEvent)──►
  server.CreateInteraction                          server.go:56
    markInFlight(conversation_id)                   server.go:62, 112-127
      already present → FailedPrecondition          server.go:63-65
    outputHandler = stream.Send                     server.go:68-70
    controller.Exec(stream.Context(), req, h)       server.go:71
      reject empty conversation_id                  controller.go:68-70
      l := newLogger(eventLog, convID, req.AgentId) controller.go:76
      l.ResumptionState()  → Events() full scan     controller.go:77, 224-241
      harness id: req → stored → registry default   controller.go:84-96
      registry.Harness(id)                          controller.go:98, registry.go:55-63
      hhandler := &harnessHandler{l, outputHandler} controller.go:103-106

      [A] if state == PENDING:                      controller.go:108-121
            exec := h.Start(ctx, convID, req.AgentConfig)
            defer exec.Close
            exec.Run(ctx, hhandler)        ← NO Queue before Run (G3)
            error → return                          controller.go:118-120
      [B] if len(req.Inputs) == 0 → return nil      controller.go:123-126
      [C] exec := h.Start(ctx, convID, req.AgentConfig)   controller.go:128
          defer exec.Close                          controller.go:132
          exec.Queue(req.Inputs...)                 controller.go:134
          l.LogInputs(inputs, agentConfig)          controller.go:138  (error returned)
          exec.Run(ctx, hhandler)                   controller.go:141
```

Inside `Run`, for a gRPC-backed harness:

```
Execution.Run
  client.Connect(ctx)                               antigravity.go:205 / substrate.go:203
  stream.Send(HarnessRequest{start{config, inputs}}) antigravity.go:212 / substrate.go:222
  stream.CloseSend()                                antigravity.go:215 / substrate.go:227
  harness.DrainStream(ctx, stream, execID, handler) stream.go:27
    Recv loop                                       stream.go:32-59
      Outputs → handler.OnMessage per step          stream.go:42-47
      End     → record state / error                stream.go:48-57
    no End frame → error                            stream.go:61-63
    End=FAILED → return error (no OnComplete)       stream.go:64-66
    else       → handler.OnComplete                 stream.go:67
```

Handler → event log:

```
harnessHandler.OnMessage                            controller.go:152
  l.LogOutputs([step], PENDING)                     controller.go:155, 267-275
    eventLog.Append(StepEvent)                      eventlog/sql.go:36
      BEGIN                                         sql.go:40
      SELECT COALESCE(MAX(step),0)+1                sql.go:46
      INSERT (conversation_id, step, payload)       sql.go:55-57
      COMMIT                                        sql.go:61
  append error → slog.Warn, continue  (I9)          controller.go:156-161
  step.Index = logStep  (0 on failure)              controller.go:163-165
  execHandler(CreateInteractionResponse{step})      controller.go:170-172
    → stream.Send to client                         server.go:69

harnessHandler.OnComplete                           controller.go:175
  l.LogOutputs(nil, COMPLETED)                      controller.go:177
  append error → slog.Warn, swallowed  (I9)         controller.go:177-182
```

All `OnMessage` calls for one `Exec` happen on the `Run` goroutine
(`stream.go:44` for gRPC harnesses, `antigravityinteractions.go:398, 446` for
the in-process one), so appends within one `Exec` are serialized. Concurrency on
a conversation only arises **across** `Exec` calls — see §2.1.

### 1.3 Rows written per `Exec`

What lands in `conversation_log` for a fresh conversation, one input, harness
emits N steps and completes:

| step | written by | `agent_id` | `agent_config` | `steps` | `state` |
|---|---|---|---|---|---|
| 1 | `LogInputs` `controller.go:256-264` | harness id | parsed `Struct` (or nil if not JSON, `:248-254`) | the inputs | `PENDING` |
| 2…N+1 | `OnMessage` → `LogOutputs` | — | — | one step each | `PENDING` |
| N+2 | `OnComplete` → `LogOutputs` | — | — | none | `COMPLETED` |

The controller writes **only `PENDING` and `COMPLETED`**
(`controller.go:155, 177, 262` are the only `State_` writes in
`internal/controller` and `internal/server`). `FAILED` and `CANCELED` are never
logged. Consequences in §5.1.

`interaction_id` is set from `l.interactionID` (`controller.go:258, 270`) which
is never assigned (`controller.go:207, 216-220`) — G11.

The persisted `payload` never carries `Step.index`: `Index` is stamped onto the
step **after** `Append` returns (`controller.go:163-165`). The row's `step`
column is the only index.

---

## 2. Component responsibilities and boundaries

### 2.1 `inFlight` map — `internal/server/server.go:43-44, 112-127`

- **Does:** rejects a second concurrent `CreateInteraction` for the same
  `conversation_id` **in the same server process** with `FailedPrecondition`
  (`server.go:62-65`). Entry removed by the deferred cleanup when `Exec`
  returns (`server.go:66, 122-126`).
- **Does not:** cover other replicas, the in-process `ax exec` path
  (`exec.go:344` calls `Exec` directly), or a second `ax serve` sharing the same
  SQLite/Postgres log. There is no lease, generation, or ownership column
  anywhere (G4).
- **Reconnect behaviour:** the entry lives until `Exec` unwinds. A client that
  disconnects and immediately retries can hit `FailedPrecondition` while the
  first `Exec` is still returning (`ARCHITECTURE.md` §6.3).
- **What actually serializes appends across writers:** the database, not the
  map. SQLite opens with `_txlock=immediate` (`sqlite.go:36`), so `BEGIN`
  takes the write lock and `MAX(step)+1` is computed under it. Postgres uses the
  default isolation (`sql.go:40`, `BeginTx(ctx, nil)`): two concurrent appends
  can compute the same `MAX+1`, and the loser fails on the
  `PRIMARY KEY (conversation_id, step)` (`postgres.go:49`) — an error that
  `OnMessage` then swallows (§2.4).

### 2.2 `ResumptionState` — `controller.go:224-241`

- Reads **every** event for the conversation (`sql.go:73`, no limit — G6).
- Returns `(last non-UNSPECIFIED state, first non-empty AgentId)`
  (`controller.go:232-239`). Discards `Steps`, `AgentConfig`, `InteractionId`.
- The returned harness id pins the conversation to its first harness
  (`controller.go:84-86`) and is reused when the request omits `agent_id`
  (`controller.go:89-91`).
- Not a rebuild: nothing it returns can be `Queue`d. This is the hook point for
  the reconciler's `rebuild(events)` (G3, `ARCHITECTURE.md` §6.1).

### 2.3 `LogInputs` / `LogOutputs` — `controller.go:243-275`

| | `LogInputs` | `LogOutputs` |
|---|---|---|
| Caller | `Exec` path [C] only, `controller.go:138` | `OnMessage` `:155`, `OnComplete` `:177` |
| `AgentId` / `AgentConfig` | set (`:259-260`) | not set (`:268-273`) |
| `State` | always `PENDING` (`:262`) | caller-supplied |
| Error handling at caller | **returned** — `Exec` fails (`:138-140`) | **logged and dropped** (`:156-161`, `:177-182`) |
| Timing | after `Queue`, before `Run` — input is durable before the harness sees a turn start | after the harness produced the step |

`agent_config` that is not valid JSON is logged as `nil` with a warning
(`controller.go:248-254`), but the raw bytes still go to the harness via
`Start(…, req.AgentConfig)` (`controller.go:128`). The log therefore cannot
reproduce a non-JSON config.

On path [A] (resume), the stored `agent_config` is **not** read back; `Start`
receives `req.AgentConfig` from the new request (`controller.go:112`), which is
empty for a bare `--resume`.

### 2.4 `harnessHandler` — `controller.go:147-187`

The single adapter between `harness.Handler` and the controller.

- `OnMessage`: append → stamp `Index` → forward to client. **Append failure
  does not stop delivery** (I9). The client sees `Index = 0`, and both CLI
  paths silently skip index 0 (`cmd/ax/exec.go:330-332, 350` local;
  `exec.go:388-390, 403` remote).
- `OnMessage` returns the client-send error (`controller.go:170-172`), so a
  disconnected client aborts the harness turn via `DrainStream`
  (`stream.go:44-46`).
- `OnComplete`: append `COMPLETED`, swallow the error (`controller.go:177-182`).
  The `execHandler == nil` branch at `:183-185` is dead (both branches return
  nil).
- `execID` is ignored by both methods.

### 2.5 `EventLog` — `eventlog/eventlog.go:31-40`, `eventlog/sql.go`

`Append` / `Events` / `Close`. One table, `conversation_log(conversation_id,
step, payload)`; payload is protojson with proto field names
(`eventlog.go:42`). One span per call (`sql.go:37, 70, 104-118`).
`eventlogtest.MemoryEventLog` mirrors the interface for tests and stores the
`*StepEvent` pointer itself (`eventlogtest/eventlog.go:29-41`), so unlike the
SQL log it observes the later `Index` mutation.

### 2.6 Lifetime and cancellation

- `Exec` runs on the gRPC stream context (`server.go:57, 71`). **Client
  disconnect cancels the harness turn**; there is no detached execution. "A
  session with no client attached still finishes" is not possible today.
- Neither gRPC harness ever sends `HarnessRequest{cancel}`: both `CloseSend`
  right after `start` (`antigravity.go:215`, `substrate.go:227`). Cancellation is
  context cancellation only. The Python sidecar ignores cancel frames anyway
  (`python/antigravity/harness_server.py:209`).
- `GracefulStop` closes the controller (and with it the event log DB) **before**
  `grpcServer.GracefulStop()` drains in-flight RPCs (`server.go:104-109`).
  In-flight `Exec`s then append to a closed DB and the errors are swallowed by
  `OnMessage`/`OnComplete`. `serve.go:93` closes the controller a second time on
  return.

---

## 3. Harnesses: Start / Queue / Run / Close

### 3.1 Behaviour matrix

| | `antigravity` (local sidecar) | `substrate` (remote actor) | `antigravityinteractions` (in-process) |
|---|---|---|---|
| Constructed | `antigravity.go:70-113`; forks Python sidecar when `autoStart` (`:86-102`), installs its own SIGINT/SIGTERM stop (`:106-111`) | `substrate.go:59-84`; ATE control client with `InsecureSkipVerify` TLS (`:69-70`); data-plane dial opts default insecure + `otelgrpc` (`:74-77`) | `antigravityinteractions.go:209-240`; HTTP client without keep-alives (`:228-230`); requires `StateDir` (`:232-238`) |
| **Start** | No I/O. Allocates exec with new UUID (`:124-131`) | `CreateActor` (AlreadyExists ok, `:94-96`) → `ResumeActor` (`:99-110`) → `grpc.NewClient(podIP:port)` (`:112-116`) → `waitForHealthy` ≤60 s (`:120-123`, `:139-169`) | Loads resume cursor from disk into `prevInteractionID` (`:253-264`) |
| **Queue** | Append to `queued`; error if closed (`:151-159`) | Append to `pending`; **no closed check** (`:187-192`) | Append to `queued`; error if closed (`:296-304`) |
| **Run: empty input** | Local error *"no input messages queued for execution turn"* (`:176-178`) | **No local check** — sends `start` with zero steps (`:198-218`); the remote decides (Python sidecar ends `FAILED`, `harness_server.py:237-247`) | *"no input messages queued for the initial turn"* / *"Run called with no queued input and no work pending"* (`:370-376`) |
| **Run: transport** | Dials per `Run` with `grpc.DialContext` (`:181-187`), `Connect`, `start`, `CloseSend`, `DrainStream` (`:190-220`) | Reuses the `Start`-time conn (`:203`), `start`, `CloseSend`, `DrainStream` (`:209-232`) | HTTPS + SSE to Vertex Interactions API; `postTurn` with 429 retry (`:387, 427, 730-758`) |
| `HarnessRequest.agent_id` | hardcoded `"antigravity"` (`:195`) | registered harness id (`:211`) | n/a |
| Run span | tracer `antigravity-harness`, span `Run` (`:163`) | tracer `substrate-harness`, span `Run` (`:195`) | none |
| **Close** | Sets `closed`; leaves sidecar running (`:224-229`) | Closes conn, `SuspendActor` with a fresh 10 s context; error only logged (`:235-256`) | Sets `closed` (`:307-312`) |
| Step types emitted | `ContentStep`, `ThoughtStep`, `ToolCallStep` with provider id (`harness_server.py:352, 377-385`); **no `ToolResultStep`** | whatever the remote emits | **only `ContentStep`** via `emitText` (`:398, 442-456`) — G1 |
| Who runs tools | Python sidecar (Antigravity SDK) | remote actor | in-process `executeTool` (`:408`, `antigravityinteractions_tools.go:42`) |

### 3.2 Where each harness keeps state

| Harness | State | Location | Who reads it |
|---|---|---|---|
| `antigravity` | SDK trajectory, keyed by AX conversation id | `<AXAssetsDir>/antigravity/conversations/<conv>` (`antigravity.go:51-57`, `config.go:366-375`), passed as `--state-dir` (`antigravity.go:83-85`); per-conversation `save_dir` (`harness_server.py:188-190`) | Python sidecar only |
| `substrate` | Actor lifecycle (create/resume/suspend) | Agent Substrate control plane via `ate.Client` (`substrate.go:94-110, 248`) | the actor; any durable state is inside it (`AX_DURABLE_DIR`, `config.go:367-368`) |
| `antigravityinteractions` | `prevInteractionID` (remote chain cursor) | in-memory (`:286`) + JSON file `<StateDir>/<sha256(conv)>.json`, fsync+rename, last-write-wins (`cursorstore.go:63-66, 86-119`); `<AXAssetsDir>/antigravityinteractions/cursors` (`:178-184`) | this harness only; **not** the event log (G2, G11) |
| `antigravityinteractions` | Conversation history | remote Vertex Interactions service, `store: true` (`:693`), chained by `previous_interaction_id` (`:697`) | the remote service |
| all | Queued input | `Execution` struct field, process memory | lost on crash |

Common to all three: **no harness state lives in the event log.** The log
holds inputs, emitted steps, and two state values (§1.3).

### 3.3 The tool loop in `antigravityinteractions.Run` — `antigravityinteractions.go:351-439`

```
drainQueue → input (error if empty)         :370-376
postTurn(input, prevID)                      :387
setPrevID(res.interactionID)  ← fsync file   :391-394, 329-341
loop turn < MaxTurns (100, :150-152):
  emitText(res.modelText) → OnMessage        :398
  for call in res.toolCalls:
    out := executeTool(call)   ← side effect :408
    next += toolResultStep  (memory only)    :410-415
  next += drainQueue()                       :419
  len(next)==0 → OnComplete                  :423-425
  postTurn(next, prevID)                     :427
  setPrevID(...)                             :431-434
turn cap → OnComplete                        :438
```

The cursor is persisted after the turn that *yields* the tool calls and before
any tool runs (`:391-394` precede `:408`) — the G2 window.

When this harness is served over gRPC (`ax harness`, substrate mode), the
adapter is `antigravityinteractions/server.go`: `Connect` → `Start` → `Queue`
the start steps if any (`:137-141`) → `Run` with a `streamHandler` that forwards
each `OnMessage` as an `outputs` frame (`:166-206`) → `sendEnd`
(`:168-176, 216-225`). Input after `start` is unsupported by design
(`:143-151`).

---

## 4. The duplicated `HarnessService` client (task 2 target)

Both gRPC-backed executions contain the same turn body; only `DrainStream` is
shared.

| Step | `antigravity.go` | `substrate.go` |
|---|---|---|
| span | `:163-164` | `:195-196` |
| take queued input | `:166-174` (with closed check) | `:198-201` |
| empty-input guard | `:176-178` | — |
| dial | `:180-187` (per Run) | in `Start` `:112-116` |
| client | `:190` | `Start` `:130` |
| build `HarnessRequest{start}` | `:193-202` | `:209-218` |
| `Connect` | `:205-208` | `:203-206` |
| `Send` + `CloseSend`, ignoring `io.EOF` | `:209-217` | `:219-229` |
| `DrainStream` | `:220` | `:232` |

The identical part is: `Connect` → build `start` → `Send` → `CloseSend` →
`DrainStream`. Dial timing (per-Run vs. per-Start), the empty-input guard, the
closed flag, and the `agent_id` value differ and stay in each harness.
Error strings differ (`"failed to call gRPC HarnessService.Connect"` vs.
`"failed to open harness service stream"`); no test asserts either
(`antigravity_test.go:95` and `substrate_test.go:287` check only the
`DrainStream` failure text).

A third, hand-rolled client exists in the hidden debug command
`cmd/ax/harnessclient.go:57-135` (its own Recv loop, no `DrainStream`). It is
outside the two-harness extraction.

---

## 5. Behaviour the test suite does not expose

These follow from the code above; they sharpen, not change, the gaps in
`PLAN.md` §2.

### 5.1 A failed or canceled turn wedges the conversation

1. Any `Run` error returns from `Exec` without writing a state
   (`controller.go:118-120, 141-143`); `DrainStream` skips `OnComplete` on
   `FAILED` (`stream.go:64-66`); client disconnect cancels the turn (§2.6);
   `OnComplete`'s own append error is swallowed (`controller.go:177-182`).
2. The last logged state is therefore still `PENDING` (§1.3).
3. Every later `Exec` takes path [A]: `Start` + `Run` with nothing queued
   (`controller.go:108-121`).
4. All three real harnesses fail that `Run` (§3.1, "Run: empty input"), and
   the error returns **before** path [C] — so even a request carrying new
   inputs cannot proceed (`controller.go:118-120` precedes `:123`).

`TestController2_ExecResumptionFlow` (`controller_test.go:290-486`) passes only
because its mock `Run` ignores empty input (`controller_test.go:368-370`,
`:432-434`). This is the concrete form of G3 and the first thing the
reconciler's `Queue()`-before-`Run()` and a written terminal state must fix.

### 5.2 Graceful shutdown loses tail appends

§2.6: event log closed before RPC drain (`server.go:104-109`). Relevant to the
`controller_kill_graceful` chaos class and to I9.

### 5.3 G1 is harness-specific

The Python `antigravity` sidecar already emits `ToolCallStep` (with the
SDK's id, `harness_server.py:376-386`); only `ToolResultStep` is missing.
`antigravityinteractions` emits neither. Neither id is usable as Conductor's
`call_id` (`ARCHITECTURE.md` §2.4).

---

## 6. Edit points for later milestones

| Milestone / item | Touch | Notes |
|---|---|---|
| `internal/harness/remote` (PLAN §6 step 1) | `antigravity.go:162-221`, `substrate.go:194-233`, `stream.go:27-68` | §4 |
| `fake` / `openaicompat` registration + `recovery: full\|none` | `controller/registry.go:25-63`, `cliutil.go:54-196` | registry has no per-harness metadata today |
| G1 tool lifecycle emission | `antigravityinteractions.go:398-416` | emit via `handler.OnMessage`, interfaces unchanged |
| I9: stop delivering unpersisted steps | `controller.go:155-165`, `:177-182` | return the error |
| I10: `event_id` idempotent append | `eventlog/sql.go:36-66`, `eventlog/eventlog.go:31-40`, `proto/ax.proto:30-37` (additive field; regenerate with `protoc`, never `sed`) | |
| Terminal state on failure (§5.1) | `controller.go:118-120, 141-143` | needs a `FAILED`/`CANCELED` write |
| G3 rebuild + `Queue` before `Run` | `controller.go:72-121`, `:224-241` | replace `ResumptionState` with `rebuild(events)` |
| G4 lease + fencing | `server.go:43-44, 62-66`; every `Append` call site (`controller.go:264, 274`) | `inFlight` becomes a same-process fast path |
| `runs` projection, `generation` column, migrations | `eventlog/sqlite.go:43-52`, `eventlog/postgres.go:44-53` (inline DDL moves to `db/migrations/`) | |
| Detached execution (no client needed) | `server.go:57, 71`, `controller.go:170-172` | today the run dies with the stream |
| Graceful drain order | `server.go:102-110`, `cmd/ax/serve.go:93` | §5.2 |
| G11 `interaction_id` | `controller.go:207, 258, 270` | known path, not scheduled (`correctness.md` I6) |
| Rename `ax` → `conductor` | `config.go:30-43`, `config.go:366-375`, `antigravityinteractions.go:154, 381`, `substrate.go:63-68`, `cmd/ax/serve.go:39-45, 78` | PLAN §6 / handoff step 6 |
