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

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.database.postgresql.PostgreSQLDatabase;

import java.sql.Connection;

/**
 * Aurora DSQL database implementation for Flyway.
 *
 * <p>Extends PostgreSQL with DSQL-specific behavior: disables DDL transactions, uses an
 * inline primary key in the schema-history create script, and keeps DDL and DML in
 * separate transactions.
 *
 * <p>Baseline: use {@code baselineOnMigrate} rather than the standalone {@code baseline}
 * command. DSQL forbids DDL and DML in one transaction, so the create-table and the baseline
 * marker insert must run in separate transactions. {@code baselineOnMigrate} does this (the
 * marker is inserted after the table is created), whereas the {@code baseline} command relies
 * on a single-transaction create script carrying the marker insert, which DSQL rejects.
 */
public class DSQLDatabase extends PostgreSQLDatabase {

    public DSQLDatabase(Configuration configuration,
                        JdbcConnectionFactory jdbcConnectionFactory,
                        StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected DSQLConnection doGetConnection(Connection connection) {
        return new DSQLConnection(this, connection);
    }

    @Override
    public boolean supportsDdlTransactions() {
        // DSQL allows only one DDL statement per transaction and cannot mix DDL and DML.
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        // DSQL does not support ALTER TABLE ADD CONSTRAINT, so the primary key is defined
        // inline. Only one DDL statement is allowed per transaction, so no CREATE INDEX is
        // bundled here. The baseline marker is intentionally never appended (the baseline flag
        // is ignored): DSQL forbids DDL and DML in one transaction, so a create script carrying
        // the marker insert would be rejected. baselineOnMigrate inserts the marker in a separate
        // transaction instead; the standalone baseline command is unsupported on DSQL.
        return "CREATE TABLE " + table + " (\n" +
               "    \"installed_rank\" INT NOT NULL PRIMARY KEY,\n" +
               "    \"version\" VARCHAR(50),\n" +
               "    \"description\" VARCHAR(200) NOT NULL,\n" +
               "    \"type\" VARCHAR(20) NOT NULL,\n" +
               "    \"script\" VARCHAR(1000) NOT NULL,\n" +
               "    \"checksum\" INT,\n" +
               "    \"installed_by\" VARCHAR(100) NOT NULL,\n" +
               "    \"installed_on\" TIMESTAMP NOT NULL DEFAULT now(),\n" +
               "    \"execution_time\" INT NOT NULL,\n" +
               "    \"success\" BOOLEAN NOT NULL\n" +
               ")";
    }

    @Override
    public boolean useSingleConnection() {
        // DSQL needs DDL and DML in separate transactions.
        return false;
    }
}
