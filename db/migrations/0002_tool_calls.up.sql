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

-- Tool ledger (docs/conductor/ARCHITECTURE.md §2.4). The log of record for
-- tool calls: one row per Conductor-computed call_id.
CREATE TABLE tool_calls (
    call_id            TEXT        PRIMARY KEY,
    conversation_id    TEXT        NOT NULL,
    step               BIGINT      NOT NULL,
    tool_name          TEXT        NOT NULL,
    args_hash          TEXT        NOT NULL,
    args               JSONB       NOT NULL,
    idempotency_key    TEXT        NOT NULL,
    class              TEXT        NOT NULL,
    state              TEXT        NOT NULL,
    attempts           INT         NOT NULL DEFAULT 0,
    reconcile_deadline TIMESTAMPTZ,
    result             JSONB,
    error              TEXT,
    started_at         TIMESTAMPTZ,
    settled_at         TIMESTAMPTZ,
    generation         BIGINT      NOT NULL
);
CREATE INDEX tool_calls_conv      ON tool_calls (conversation_id, step);
CREATE INDEX tool_calls_unsettled ON tool_calls (state, reconcile_deadline)
  WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN');
