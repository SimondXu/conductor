# Refund agent under failure

An end-to-end test of the owned loop with a model in it. A small refund agent
runs on real `conductor serve` processes against PostgreSQL. The test kills
those processes at chosen points and checks what recovery did.

**The agent.** It gets one task: order 1042 arrived damaged, so look it up,
refund it, and email the customer. It has three tools, served by the test:

| Tool | Class | Backend behaviour |
|---|---|---|
| `lookup_order` | `IDEMPOTENT` | Read-only. |
| `refund_payment` | `IDEMPOTENT` | Honours `Idempotency-Key`: a repeated key returns the first refund. |
| `send_email` | `UNSAFE` | No key. A second request is a second email. |

The backend counts real effects: refunds by key, and emails.

**The breakpoints.**

- `TestKillAtBoundary`: `CONDUCTOR_CHAOS_KILL_AT` stops the controller at
  `model_started`, `model_completed`, and at `tool_planned`, `tool_started`
  and `tool_settled` for `refund_payment`. A fresh replica recovers the run.
- `TestRefundDoneResultLost`: the backend refunds, then withholds the
  response, and the controller is SIGKILLed.
- `TestEmailSentResultLost`: the same, for the email.
- `TestGracefulHandover`: SIGTERM to the owner during a slow refund, with a
  second replica running.
- `TestOwnerKilledTakeover`: SIGKILL to the owner after the refund's effect,
  with a second replica running.

Every scenario checks the run's end state, the real refund and email counts,
and every invariant in `docs/conductor/correctness.md` over its conversation.
The expected outcome is one refund and one email, except when the email's
result is lost: `send_email` is `UNSAFE`, so it is not sent again and the run
ends `FAILED` with one email out.

## Running it

It needs the PostgreSQL that the other integration tests use. It builds
`conductor` itself.

```bash
CONDUCTOR_E2E_AGENT=mock PG_TEST_DSN='postgres://postgres:test@127.0.0.1:55432/postgres?sslmode=disable' go test -count=1 -v ./test/agent/
```

**Mock model.** `mock` uses a scripted Messages API in the test. It makes one
tool call per turn, each led by a thinking block. It also runs the check the
real API runs on thinking blocks: the signature is a hash of everything before
the block, so a request whose history changed since the block was made gets the
real API's 400. It costs nothing and takes about 40 s.

**Real model.** `anthropic` sends the same scenarios to the Anthropic Messages
API through the `anthropic` harness. It reads `ANTHROPIC_API_KEY` and uses
`claude-haiku-5-5` unless `CONDUCTOR_E2E_MODEL` names another model. A real
model chooses its own calls, so a scenario can fail because the model did
something other than the task, for example two calls in one turn. Read the
log before calling that a Conductor bug.

**Budget.** All model traffic goes through a meter in the test.

- It refuses requests past `CONDUCTOR_E2E_MAX_REQUESTS`, 150 by default.
- It stops everything on the first 401, 403 or 429.
- It prints the requests and tokens used at the end.

A full mock run makes 39 model requests. A real run makes about as many, plus
the retries of any failed turn.

It is not run by `go test ./...`: without `CONDUCTOR_E2E_AGENT` it skips.
