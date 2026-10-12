#!/usr/bin/env bash
# Copyright 2026 Simon Xu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end check of the compose stack with zero API keys: bring it up, run
# the fake script once through the server, and assert on Postgres and
# Prometheus. Used locally and in CI. Leaves the stack running.
set -euo pipefail
cd "$(dirname "$0")"

dc() { docker compose -f docker-compose.yml "$@"; }
psql() { dc exec -T postgres psql -U conductor -d conductor -tAc "$1"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

dc up -d --build --wait

conv="smoke-$(date +%s)"
dc exec -T conductor conductor run --server localhost:8494 --conversation "$conv" --input "book me a flight"

state=$(psql "SELECT payload::jsonb->>'state' FROM conversation_log WHERE conversation_id = '$conv' ORDER BY step DESC LIMIT 1")
[[ "$state" == "STATE_COMPLETED" ]] || fail "final state is '$state', want STATE_COMPLETED"

steps=$(psql "SELECT count(*) FROM conversation_log WHERE conversation_id = '$conv'")
[[ "$steps" == "8" ]] || fail "conversation has $steps log rows, want 8"

effects=$(psql "SELECT count(*) FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.conversation_id = '$conv'")
[[ "$effects" == "2" ]] || fail "fake_effects has $effects rows for $conv, want 2"

settled=$(psql "SELECT count(*) FROM tool_calls WHERE conversation_id = '$conv' AND state = 'SUCCEEDED' AND attempts = 1")
[[ "$settled" == "2" ]] || fail "tool_calls has $settled settled calls for $conv, want 2"

# Crash and resume (M4): the fake harness dies after charge_card's effect and
# before its result. The run fails; --resume, with no input and no
# agent_config, runs the IDEMPOTENT call again under the same key and
# completes.
crash="crash-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"crash_after_effect":true}}}'
if dc exec -T conductor conductor run --server localhost:8494 --conversation "$crash" --agent-config "$cfg" --input "charge me"; then
  fail "the crashing run succeeded"
fi
for _ in $(seq 1 30); do
  [[ "$(dc ps --format '{{.State}}' fake-harness)" == "running" ]] && break
  sleep 1
done
dc exec -T conductor conductor run --server localhost:8494 --conversation "$crash" --resume

state=$(psql "SELECT payload::jsonb->>'state' FROM conversation_log WHERE conversation_id = '$crash' ORDER BY step DESC LIMIT 1")
[[ "$state" == "STATE_COMPLETED" ]] || fail "resumed run's final state is '$state', want STATE_COMPLETED"
call=$(psql "SELECT state || ' ' || attempts FROM tool_calls WHERE conversation_id = '$crash'")
[[ "$call" == "SUCCEEDED 2" ]] || fail "resumed call is '$call', want 'SUCCEEDED 2'"
effects=$(psql "SELECT count(*) FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.conversation_id = '$crash'")
[[ "$effects" == "1" ]] || fail "resumed run has $effects effects, want 1"

# Falsifying queries from docs/conductor/correctness.md; each must be empty.
check() {
  local rows
  rows=$(psql "$2")
  [[ -z "$rows" ]] || fail "$1 violated: $rows"
}
check I1 "SELECT call_id FROM tool_calls WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN') AND reconcile_deadline < now()"
check I2 "SELECT e.idempotency_key FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.class = 'IDEMPOTENT' GROUP BY e.idempotency_key HAVING count(*) > 1"
check I7b "SELECT t.call_id FROM tool_calls t WHERE t.state = 'SUCCEEDED' AND NOT EXISTS (
  SELECT 1 FROM conversation_log l WHERE l.conversation_id = t.conversation_id
     AND l.payload::jsonb @> jsonb_build_object('steps', jsonb_build_array(
           jsonb_build_object('tool_result', jsonb_build_object('call_id', t.call_id)))))"

# Prometheus scrapes every 5s; give it a few rounds to see the run.
for _ in $(seq 1 12); do
  runs=$(dc exec -T prometheus wget -qO- 'http://localhost:9090/api/v1/query?query=sum(conductor_runs_total{outcome="completed"})' \
    | sed -n 's/.*"value":\[[^,]*,"\([0-9.]*\)"\].*/\1/p')
  [[ -n "$runs" && "$runs" != "0" ]] && break
  sleep 5
done
[[ -n "$runs" && "$runs" != "0" ]] || fail "Prometheus has no completed runs"

echo "PASS: $conv completed with $steps log rows and $settled settled calls; $crash resumed after a crash with one effect over two attempts; I1, I2, I7b hold; Prometheus sees $runs completed run(s)"
