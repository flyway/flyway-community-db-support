---
-- ========================LICENSE_START=================================
-- flyway-database-duckdb
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
CREATE TABLE ${flyway:defaultSchema}.some_table(
    id INT,
    text VARCHAR
);

CREATE INDEX some_table_text ON ${flyway:defaultSchema}.some_table(text);

CREATE SEQUENCE some_sequence;

INSERT INTO ${flyway:defaultSchema}.some_table(id, text) VALUES (1, 'first');
INSERT INTO ${flyway:defaultSchema}.some_table(id, text) VALUES (2, 'second');
