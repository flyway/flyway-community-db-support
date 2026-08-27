---
-- ========================LICENSE_START=================================
-- flyway-database-questdb
-- ========================================================================
-- Copyright (C) 2010 - 2025 Red Gate Software Ltd
-- ========================================================================
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- =========================LICENSE_END==================================
---
-- The rename is exercised on its own table so that `trades` is never renamed. QuestDB's
-- asynchronous column conversion can complete after a DROP holding an earlier table name,
-- which re-registers the dropped table under that name and leaves a phantom behind.
CREATE TABLE rename_source (
    instrument SYMBOL,
    ts TIMESTAMP
) TIMESTAMP(ts) PARTITION BY DAY WAL;

INSERT INTO rename_source (instrument, ts) values ('SYM1', '2025-05-09T00:01:00.000000Z');

RENAME TABLE rename_source TO rename_target;
