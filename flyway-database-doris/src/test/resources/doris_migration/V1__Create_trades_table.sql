---
-- ========================LICENSE_START=================================
-- flyway-database-doris
-- ========================================================================
-- Copyright (C) 2010 - 2026 Red Gate Software Ltd
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
CREATE TABLE trades (
    `id` INT NOT NULL,
    `symbol` VARCHAR(50),
    `price` DECIMAL(10,2),
    `ts` DATETIME NOT NULL
)
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
