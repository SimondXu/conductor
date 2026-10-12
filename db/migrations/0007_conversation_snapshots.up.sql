-- Copyright 2026 Simon Xu
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.


-- Replay snapshots (PLAN.md G6): the conversation's events 1..step folded
-- into one StepEvent, binary protobuf, the latest one per conversation. A
-- cache, never authoritative: the log is append-only, so the fold of its
-- first `step` events is the same whoever computes it, and the log itself
-- is kept whole.
CREATE TABLE conversation_snapshots (
    conversation_id TEXT        PRIMARY KEY,
    step            BIGINT      NOT NULL,
    event           BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
