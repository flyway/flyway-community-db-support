/*-
 * ========================LICENSE_START=================================
 * flyway-database-starrocks
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

package org.flywaydb.community.database.starrocks;

import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;

import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * StarRocks connection.
 * 
 * Extends the base Connection class directly instead of MySQLConnection
 * because StarRocks doesn't support many MySQL-specific system variables
 * like 'foreign_key_checks' that MySQLConnection tries to query.
 */
public class StarRocksConnection extends Connection<StarRocksDatabase> {

    public StarRocksConnection(StarRocksDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT DATABASE()");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        jdbcTemplate.execute("USE " + database.quote(schema));
    }

    @Override
    public Schema getSchema(String name) {
        return new StarRocksSchema(jdbcTemplate, database, name);
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        // StarRocks doesn't support traditional locking mechanisms like MySQL's GET_LOCK
        // We proceed without locking, but this means concurrent migrations may cause issues
        try {
            return callable.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
