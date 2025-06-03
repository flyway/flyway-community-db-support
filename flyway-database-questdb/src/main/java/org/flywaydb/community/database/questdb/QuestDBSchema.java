/*-
 * ========================LICENSE_START=================================
 * flyway-database-questdb
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
 * ========================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.flywaydb.community.database.questdb;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

public class QuestDBSchema extends Schema<QuestDBDatabase, QuestDBTable> {

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database The database-specific support.
     * @param name The name of the schema.
     */
    public QuestDBSchema(JdbcTemplate jdbcTemplate, QuestDBDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() {
        return true;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        final List<String> tableNames =
                jdbcTemplate.queryForStringList("show tables").stream()
                        .filter(tbl -> !isSystem(tbl))
                        .toList();
        return tableNames.isEmpty();
    }

    private boolean isSystem(String tableName) {
        return tableName.startsWith("sys.")
                || tableName.equalsIgnoreCase("telemetry_config")
                || tableName.equalsIgnoreCase("telemetry")
                || tableName.equalsIgnoreCase("_query_trace");
    }

    @Override
    protected void doCreate() {
    }

    @Override
    protected void doDrop() {
    }

    @Override
    protected void doClean() {
        for (QuestDBTable table : allTables()) {
            table.drop();
        }
    }

    @Override
    protected QuestDBTable[] doAllTables() throws SQLException {
        List<String> tableNames =
                jdbcTemplate.queryForStringList("show tables").stream()
                        .filter(tbl -> !isSystem(tbl))
                        .toList();
        QuestDBTable[] tables = new QuestDBTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new QuestDBTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public QuestDBTable getTable(String tableName) {
        return new QuestDBTable(jdbcTemplate, database, this, tableName);
    }
}
