/*-
 * ========================LICENSE_START=================================
 * flyway-database-doris
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

package org.flywaydb.community.database.doris;

import lombok.CustomLog;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

@CustomLog
public class DorisConnection extends Connection<DorisDatabase> {
    private static final String USER_VARIABLES_TABLE = "performance_schema.user_variables_by_thread";
    private static final String SQL_SAFE_UPDATES = "sql_safe_updates";

    private final String userVariablesQuery;
    private final boolean canResetUserVariables;
    private final int originalSqlSafeUpdates;

    public DorisConnection(DorisDatabase database, java.sql.Connection connection) {
        super(database, connection);

        userVariablesQuery = "SELECT variable_name FROM " + USER_VARIABLES_TABLE + " WHERE variable_value IS NOT NULL";
        canResetUserVariables = hasUserVariableResetCapability();
        originalSqlSafeUpdates = getIntVariableValue(SQL_SAFE_UPDATES);
    }

    private int getIntVariableValue(String varName) {
        try {
            return jdbcTemplate.queryForInt("SELECT @@" + varName);
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to determine value for '" + varName + "' variable", e);
        }
    }

    private boolean hasUserVariableResetCapability() {
        try {
            jdbcTemplate.queryForStringList(userVariablesQuery);
            return true;
        } catch (SQLException e) {
            LOG.debug("Disabled user variable reset as "
                    + USER_VARIABLES_TABLE
                    + " cannot be queried (SQL State: " + e.getSQLState() + ", Error Code: " + e.getErrorCode() + ")");
            return false;
        }
    }

    @Override
    protected void doRestoreOriginalState() throws SQLException {
        resetUserVariables();
        jdbcTemplate.execute("SET " + SQL_SAFE_UPDATES + "=?", originalSqlSafeUpdates);
    }

    private void resetUserVariables() throws SQLException {
        if (canResetUserVariables) {
            List<String> userVariables = jdbcTemplate.queryForStringList(userVariablesQuery);
            if (!userVariables.isEmpty()) {
                boolean first = true;
                StringBuilder setStatement = new StringBuilder("SET ");
                for (String userVariable : userVariables) {
                    if (first) {
                        first = false;
                    } else {
                        setStatement.append(",");
                    }
                    setStatement.append("@").append(userVariable).append("=NULL");
                }
                jdbcTemplate.executeStatement(setStatement.toString());
            }
        }
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT DATABASE()");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        if (StringUtils.hasLength(schema)) {
            jdbcTemplate.getConnection().setCatalog(schema);
        } else {
            try {
                String newDb = database.quote(UUID.randomUUID().toString());
                jdbcTemplate.execute("CREATE SCHEMA " + newDb);
                jdbcTemplate.execute("USE " + newDb);
                jdbcTemplate.execute("DROP SCHEMA " + newDb);
            } catch (Exception e) {
                LOG.warn("Unable to restore connection to having no default schema: " + e.getMessage());
            }
        }
    }

    @Override
    protected Schema doGetCurrentSchema() throws SQLException {
        String schemaName = getCurrentSchemaNameOrSearchPath();
        return schemaName == null ? null : getSchema(schemaName);
    }

    @Override
    public Schema getSchema(String name) {
        return new DorisSchema(jdbcTemplate, database, name);
    }
}
