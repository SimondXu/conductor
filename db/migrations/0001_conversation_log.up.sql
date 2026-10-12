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

-- Baseline: upstream's only table. IF NOT EXISTS adopts databases created by
-- upstream ax, which made this table at startup. There is no down migration:
-- rolling back the baseline would drop the event log.
CREATE TABLE IF NOT EXISTS conversation_log (
    conversation_id TEXT    NOT NULL,
    step            INTEGER NOT NULL,
    payload         TEXT    NOT NULL,
    PRIMARY KEY (conversation_id, step)
);
