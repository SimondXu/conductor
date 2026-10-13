# Refund agent under failure

An end-to-end test of the owned loop with a model in it. A small refund agent
runs on real `conductor serve` processes against PostgreSQL. The test kills
those processes at chosen points and checks what recovery did.

**The agent.** It gets one task: order 1042 arrived damaged, so look it up,
refund it, and email the customer. It has three tools, served by the test:

| Tool | Class | Backend behaviour |
|---|---|---|
| `lookup_order` | `IDEMPOTENT` | Read-only. Echoes the order id it is asked for. |
| `refund_payment` | `IDEMPOTENT` | Honours `Idempotency-Key`: a repeated key returns the first refund. |
| `send_email` | `UNSAFE` | No key. A second request is a second email. |

The backend counts real effects: refunds by key, and emails. A scenario can
make one tool slow, answer 503 (before or after its effect), or do its effect
and withhold the response.

Every scenario checks the run's end state, the real refund and email counts,
and every invariant in `docs/conductor/correctness.md` over its conversation.
Most also check the tool ledger (`tool:STATE:attempts` per call).

## Scenarios

Breakpoints are `CONDUCTOR_CHAOS_KILL_AT=<boundary>[:<tool>]` (the process
exits 137 at the first match). "Both" means the scenario also runs against a
real model; "mock" ones script the model and skip with a real one. Requests
are model requests in mock mode.

| Scenario | Breakpoint or injection | Expected outcome | Source | Mode | Req. |
|---|---|---|---|---|---|
| `TestBaseline` | none | COMPLETED, 1 refund, 1 email | — | both | 4 |
| `TestKillAtBoundary` (5 rows) | `model_started`, `model_completed`, `tool_planned`/`tool_started`/`tool_settled:refund_payment` | COMPLETED 1/1; refund attempts 2 only for `tool_started` | ARCH §6.1 | both | 20 |
| `TestRefundDoneResultLost` | refund done, response withheld, SIGKILL | COMPLETED 1/1, 2 refund requests under one key | ARCH §2.5, I2 | both | 4 |
| `TestEmailSentResultLost` | email sent, response withheld, SIGKILL | FAILED, 1 email, never resent | PRD §6.3 | both | 3 |
| `TestGracefulHandover` | SIGTERM during a slow refund | COMPLETED 1/1 | ARCH §6.2 | both | 4 |
| `TestOwnerKilledTakeover` | SIGKILL after the refund's effect, 2 replicas | COMPLETED 1/1, generation ≥ 2 | ARCH §6.2 | both | 4 |
| `TestDoubleFault` | `tool_started:refund_payment` on the owner, then again on the replica recovering it; a third start | COMPLETED 1/1, refund attempts 3, 1 refund request, generation 3 | ARCH §6.1; PRD §6.2 step 5 | both | 4 |
| `TestKillAtUnsafeBoundary` | `tool_planned:send_email` | COMPLETED 1/1: a PLANNED call never started, so it runs | ARCH §6.1 (resolving unanswered calls) | both | 4 |
| | `tool_started:send_email` | FAILED, 1 refund, 0 emails; email ABANDONED, its error result in the FAILED event; a later input is accepted | PRD §6.3; ARCH §6.1 ("When the run fails") | both | 4 |
| | `tool_settled:send_email` | COMPLETED 1/1, the email never resent | ARCH §3 | both | 4 |
| `TestKillAfterLeaseRenewal` | `lease_renewed` (slow refund puts it inside the run) | COMPLETED 1/1; the other replica's first model request ≥ 2.5 s after the kill (the renewed lease's ttl) | ARCH §2.2; I3 | both | 4 |
| `TestFollowUpTurn` | second user input; `event_appended` (its first append) | COMPLETED, 2 refunds, 2 emails; turn-1 thinking blocks replayed intact | ARCH §1.4; PRD §3.5 | both | 8 |
| | second user input; `tool_started:refund_payment` | the same; second refund attempts 2 | ARCH §1.4, §6.1 | both | 8 |
| `TestInputDuringUnfinishedRun` | `tool_started:refund_payment`, recovery held off, new input to the other replica | input refused ("resume it first"), not logged; run then COMPLETED 1/1 | ARCH §6.1 | both | 4 |
| `TestRecoveryCrashLoop` | `tool_started:refund_payment` on every start | FAILED by the poison guard after 1 + 5 deaths, no effects, 2 requests; the history closed, so a later input is accepted | ARCH §6.1 (Poison; "When the run fails"); I6 check | both | 2 |
| `TestParallelToolCalls` | refund and email in one turn, no failure | COMPLETED 1/1 | ARCH §1.4 | mock | 3 |
| | same, `tool_started:refund_payment` | FAILED, 0/0: both calls STARTED, the UNSAFE one fails the run, the refund is ABANDONED too | ARCH §3, §6.1 | mock | 2 |
| | same, SIGKILL with the email in flight after the refund's result | FAILED, 1 refund, 0 emails; refund settled from the log, not rerun | ARCH §6.1 | mock | 2 |
| `TestToolErrorThenRetry` | refund endpoint 503 once, before its effect | COMPLETED 1/1; ledger refund FAILED then SUCCEEDED | PRD §3.2 | both | 5 |
| | 503 once, after its effect | COMPLETED, **2 refunds**: the model's retry is a new call with a new key | PRD §3.3 (documented limit) | both | 5 |
| `TestModelTransientErrors` | model 529 once | COMPLETED 1/1, 1 extra request (SDK retry) | PRD §3.5 | mock | 5 |
| | model 500 three times | SDK gives up, the turn logs nothing, recovery reruns it: COMPLETED 1/1, refund not rerun, 1 recovery | PRD §3.5; ARCH §1.4 | mock | 7 |
| `TestModelTurnFailsPersistently` | model 400 on every turn after the refund | FAILED by the poison guard, 8 requests (2 + client's turn + 5 recoveries), nothing more | ARCH §6.1 | mock | 8 |
| | `refusal` stop reason | the same; nothing of the response logged | PRD §3.5 | mock | 8 |
| | `max_tokens` with a cut-off `send_email` call | the same; the cut-off call never planned or run | PRD §3.5 | mock | 8 |
| | a model that never stops calling tools | FAILED by the poison guard after `max_turns` (24) requests; recovery grants no more | PRD §3.2 | mock | 24 |
| `TestDuplicateConcurrentRequest` | the conversation sent to both replicas at once | exactly one client succeeds, the other gets FAILED_PRECONDITION; 1 input logged, 1/1 | ARCH §6.1; correctness §2 | both | 4 |
| `TestClientReattach` | client drops at the refund call, re-attaches on the other replica with `--resume --after-step N` | every later step exactly once (gRPC and CLI), stream ends with COMPLETED 1/1 | ARCH §6.3; PRD §6.1 | both | 4 |
| `TestClientDisconnectDuringUnsafeCall` | the client is killed while the email request is in flight | COMPLETED 1/1: the run keeps going to the end of the turn | PRD §6.2 | both | 3 |
| `TestMeterAllowsOnlyHaiku` | non-Haiku model through the meter | refused locally with 400, and the run aborts | — | mock | 0 |

**Known Conductor bugs.** Two scenarios observe behaviour the docs rule out.
Each skips with `BUG (Conductor, reported)` and the evidence, and passes once
the bug is fixed:

- `TestClientDisconnectDuringUnsafeCall`: `CreateInteraction` runs `Exec` on
  the client's stream context, so a client that goes away cancels the email
  request in flight. The call goes `OUTCOME_UNKNOWN`, recovery abandons it,
  and the run ends `FAILED` with no email.
- `TestRecoveryCrashLoop`: the poison guard appends `STATE_FAILED` without an
  error result for the call left unanswered, so the history stays open (the
  I6 check fails) and every later input is refused as "an unfinished run".

The poison scenarios clear the run's `next_attempt_at` as they go, so five
recoveries take seconds rather than the 155 s of backoff. That changes when
recoveries happen, not how many a run gets.

## Running it

It needs the PostgreSQL that the other integration tests use. It builds
`conductor` itself.

```bash
CONDUCTOR_E2E_AGENT=mock PG_TEST_DSN='postgres://postgres:test@127.0.0.1:55432/postgres?sslmode=disable' go test -count=1 -v ./test/agent/
```

**Mock model.** `mock` uses a scripted Messages API in the test. It does the
task in the latest user message, one tool call per turn, each led by a
thinking block, and calls a failing tool once more before giving up. It runs
two checks the real API runs: the signature of a thinking block is a hash of
everything before it, so a request whose history changed since the block was
made gets the real API's 400; and every `tool_use` must be answered by a
`tool_result` at the start of the next message. A scenario can script it to
call two tools in one turn, never stop, or fail a turn with an HTTP error, a
refusal or a `max_tokens` stop. It costs nothing; a full run takes a little
over two minutes and makes 169 model requests.

**Real model.** `anthropic` sends the same scenarios to the Anthropic Messages
API through the `anthropic` harness. It reads `ANTHROPIC_API_KEY` and uses
`claude-haiku-5-5` unless `CONDUCTOR_E2E_MODEL` names another model.

- **Haiku only.** The key may only be used with Haiku models. The test refuses
  to start when `CONDUCTOR_E2E_MODEL` does not begin with `claude-haiku-`, and
  the meter refuses, and stops the run on, any request for another model.
- Scenarios that script the model (parallel calls, injected errors, refusals)
  skip. The rest make about 105 requests, plus the retries of any failed turn.
- A real model chooses its own calls, so a scenario can fail because the model
  did something other than the task, for example two calls in one turn. Read
  the log before calling that a Conductor bug.

**Budget.** All model traffic goes through a meter in the test.

- It refuses requests past `CONDUCTOR_E2E_MAX_REQUESTS`, 250 by default.
- It stops everything on the first 401, 403 or 429.
- It prints the requests and tokens used at the end, and each scenario logs
  its own count.

Before the run, any `e2e-agent-*` run an earlier, aborted run left `PENDING`
has its recovery deferred for a day, so this run's replicas do not adopt it.

It is not run by `go test ./...`: without `CONDUCTOR_E2E_AGENT` it skips.
