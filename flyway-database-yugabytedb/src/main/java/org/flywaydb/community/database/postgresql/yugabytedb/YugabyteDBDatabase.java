/*-
 * ========================LICENSE_START=================================
 * flyway-database-yugabytedb
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

package org.flywaydb.community.database.postgresql.yugabytedb;

import lombok.CustomLog;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.database.postgresql.PostgreSQLDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


@CustomLog
public class YugabyteDBDatabase extends PostgreSQLDatabase {

    public static final String LOCK_TABLE_NAME = "YB_FLYWAY_LOCK_TABLE";
    // Using table name in lower case, see https://github.com/flyway/flyway-community-db-support/issues/97
    private static final String LOCK_TABLE_SCHEMA_SQL = "SELECT table_name, column_name FROM information_schema.columns WHERE table_name = '" + LOCK_TABLE_NAME.toLowerCase() + "'";
    private static final String DROP_LOCK_TABLE_IF_EXISTS_DDL = "DROP TABLE IF EXISTS " + LOCK_TABLE_NAME;
    /**
     * This table is used to enforce locking through SELECT ... FOR UPDATE on a
     * token row inserted in this table. The token row is inserted with the name
     * of the Flyway's migration history table as a token for simplicity.
     */
    private static final String CREATE_LOCK_TABLE_DDL = "CREATE TABLE IF NOT EXISTS " + LOCK_TABLE_NAME + " (table_name varchar PRIMARY KEY, lock_id bigint, ts timestamp)";

    public YugabyteDBDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
        createLockTable();
    }

    @Override
    protected YugabyteDBConnection doGetConnection(Connection connection) {
        return new YugabyteDBConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
        // Checks the Postgres version
        ensureDatabaseIsRecentEnough("11.2");
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        return "CREATE TABLE IF NOT EXISTS " + table + " (\n" +
                "    \"installed_rank\" INT NOT NULL PRIMARY KEY,\n" +
                "    \"version\" VARCHAR(50),\n" +
                "    \"description\" VARCHAR(200) NOT NULL,\n" +
                "    \"type\" VARCHAR(20) NOT NULL,\n" +
                "    \"script\" VARCHAR(1000) NOT NULL,\n" +
                "    \"checksum\" INTEGER,\n" +
                "    \"installed_by\" VARCHAR(100) NOT NULL,\n" +
                "    \"installed_on\" TIMESTAMP NOT NULL DEFAULT now(),\n" +
                "    \"execution_time\" INTEGER NOT NULL,\n" +
                "    \"success\" BOOLEAN NOT NULL\n" +
                ");\n" +
                (baseline ? getBaselineStatement(table) + ";\n" : "") +
                "CREATE INDEX IF NOT EXISTS \"" + table.getName() + "_s_idx\" ON " + table + " (\"success\");";
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    private void createLockTable() {
        try {
            List<String> columns = jdbcTemplate.query(LOCK_TABLE_SCHEMA_SQL, rs -> rs.getString("column_name"));
            if (columns.isEmpty()) {
                LOG.debug("Lock table not found, creating it...");
                jdbcTemplate.execute(CREATE_LOCK_TABLE_DDL);
            } else {
                for (String column : columns) {
                    if ("lock_id".equals(column)) {
                        LOG.debug("Lock table with expected schema already exists");
                        return;
                    }
                }
                LOG.info("Lock table exists but has old schema. Dropping and recreating it with new schema...");
                jdbcTemplate.execute(DROP_LOCK_TABLE_IF_EXISTS_DDL);
                jdbcTemplate.execute(CREATE_LOCK_TABLE_DDL);
            }
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to initialize the lock table", e);
        }
    }
}
