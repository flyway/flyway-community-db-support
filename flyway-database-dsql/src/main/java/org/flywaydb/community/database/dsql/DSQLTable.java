/*-
 * ========================LICENSE_START=================================
 * flyway-database-dsql
 * ========================================================================
 * Copyright (C) 2010 - 2026 Red Gate Software Ltd
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
package org.flywaydb.community.database.dsql;

import lombok.CustomLog;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.database.postgresql.PostgreSQLSchema;
import org.flywaydb.database.postgresql.PostgreSQLTable;

import java.sql.SQLException;

/**
 * Aurora DSQL table implementation for Flyway.
 *
 * <p>Skips {@code FOR UPDATE} locking: DSQL requires an equality predicate on the key,
 * which Flyway's default locking query does not provide.
 */
@CustomLog
public class DSQLTable extends PostgreSQLTable {

    public DSQLTable(JdbcTemplate jdbcTemplate, DSQLDatabase database, PostgreSQLSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doLock() throws SQLException {
        LOG.debug("Skipping FOR UPDATE lock on table " + getName() + " (not supported by Aurora DSQL)");
    }
}
