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

-- Ownership (docs/conductor/ARCHITECTURE.md §2.2). One row per conversation,
-- never deleted: Release clears owner_id and keeps generation, so the fencing
-- token outlives the lease.
CREATE TABLE conversation_leases (
    conversation_id TEXT        PRIMARY KEY,
    owner_id        TEXT,
    generation      BIGINT      NOT NULL,
    acquired_at     TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX conversation_leases_expiry ON conversation_leases (expires_at);
