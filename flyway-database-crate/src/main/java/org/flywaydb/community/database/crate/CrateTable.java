/*-
 * ========================LICENSE_START=================================
 * flyway-database-crate
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
package org.flywaydb.community.database.crate;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class CrateTable extends Table<CrateDatabase, CrateSchema> {
    public CrateTable(JdbcTemplate jdbcTemplate, CrateDatabase database, CrateSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE " + this);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt(
                "SELECT COUNT() FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                schema.getName(), name) > 0;
    }

    @Override
    protected void doLock() throws SQLException {
        // NOOP - There is no support for locking in CrateDB
    }
}
