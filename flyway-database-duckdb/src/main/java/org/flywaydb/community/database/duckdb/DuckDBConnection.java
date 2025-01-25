/*-
 * ========================LICENSE_START=================================
 * flyway-database-duckdb
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
package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.internal.database.base.Connection;

import java.sql.SQLException;

public class DuckDBConnection extends Connection<DuckDBDatabase> {

    protected DuckDBConnection(DuckDBDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT current_schema()");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        final var sql = "USE %s;".formatted(schema);
        jdbcTemplate.execute(sql);
    }

    @Override
    public DuckDBSchema getSchema(String name) {
        return new DuckDBSchema(jdbcTemplate, database, name);
    }
}
