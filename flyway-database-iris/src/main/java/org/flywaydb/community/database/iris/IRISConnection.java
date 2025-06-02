/*-
 * ========================LICENSE_START=================================
 * flyway-database-iris
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
package org.flywaydb.community.database.iris;

import org.flywaydb.core.internal.database.base.Connection;

import java.sql.SQLException;

public class IRISConnection extends Connection<IRISDatabase> {
    protected IRISConnection(IRISDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        getJdbcConnection().setSchema(schema);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return "SQLUser";
    }

    @Override
    public IRISSchema getSchema(String name) {
        return new IRISSchema(jdbcTemplate, database, name);
    }
}
