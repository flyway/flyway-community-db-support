/*-
 * ========================LICENSE_START=================================
 * flyway-database-cubrid
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
package org.flywaydb.community.database.cubrid;

import java.sql.SQLException;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

public class CubridSchema extends Schema<CubridDatabase, CubridTable> {

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public CubridSchema(JdbcTemplate jdbcTemplate, CubridDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() {
        // Do nothing, CUBRID doesn't support schemas
        return true;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        int tableCount = jdbcTemplate.queryForInt(
            "SELECT COUNT(*) FROM db_class WHERE class_type = 'CLASS' AND is_system_class = 'NO'"
        );
        return tableCount == 0;
    }

    @Override
    protected void doCreate() {
        // Do nothing, CUBRID doesn't support schemas
    }

    @Override
    protected void doDrop() {
        // Do nothing, CUBRID doesn't support schemas
    }

    @Override
    protected void doClean() throws SQLException {
        for (CubridTable table : doAllTables()) {
            table.doDrop();
        }
    }

    @Override
    protected CubridTable[] doAllTables() throws SQLException {
        return jdbcTemplate.queryForStringList(
                "SELECT class_name FROM db_class WHERE class_type = 'CLASS' AND is_system_class = 'NO'"
            ).stream()
            .map(tableName -> new CubridTable(jdbcTemplate, database, this, tableName))
            .toArray(CubridTable[]::new);
    }

    @Override
    public CubridTable getTable(String tableName) {
        return new CubridTable(jdbcTemplate, database, this, tableName);
    }
}
