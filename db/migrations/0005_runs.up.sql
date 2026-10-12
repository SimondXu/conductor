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

-- Run index (docs/conductor/ARCHITECTURE.md §2.3): a projection of
-- conversation_log, written in the same transaction as each append, that lets
-- the reconciler find PENDING runs without scanning the log. It is never
-- authoritative: recovery re-derives the truth from the log (I7a).
-- recovery_attempts and next_attempt_at are the reconciler's own bookkeeping
-- (poison guard, backoff), not part of the projection.
CREATE TABLE runs (
    conversation_id   TEXT        PRIMARY KEY,
    state             TEXT        NOT NULL,   -- PENDING|COMPLETED|FAILED|CANCELED
    agent_id          TEXT,
    last_step         BIGINT      NOT NULL,
    recovery_attempts INT         NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX runs_pending ON runs (next_attempt_at) WHERE state = 'PENDING';

-- Backfill from the log: the last logged state, the first logged agent_id.
INSERT INTO runs (conversation_id, state, agent_id, last_step, updated_at)
SELECT c.conversation_id,
       COALESCE((SELECT replace(l.payload::jsonb->>'state', 'STATE_', '')
                   FROM conversation_log l
                  WHERE l.conversation_id = c.conversation_id AND l.payload::jsonb ? 'state'
                  ORDER BY l.step DESC LIMIT 1), 'PENDING'),
       (SELECT l.payload::jsonb->>'agent_id'
          FROM conversation_log l
         WHERE l.conversation_id = c.conversation_id AND l.payload::jsonb->>'agent_id' <> ''
         ORDER BY l.step LIMIT 1),
       max(c.step),
       now()
  FROM conversation_log c
 GROUP BY c.conversation_id;
