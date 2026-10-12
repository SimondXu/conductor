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

-- Idempotent append (correctness.md I10): a retried append carrying the same
-- event_id is a no-op. NULL for rows written before this migration.
ALTER TABLE conversation_log ADD COLUMN event_id UUID;
CREATE UNIQUE INDEX conversation_log_event_id ON conversation_log (event_id)
  WHERE event_id IS NOT NULL;
