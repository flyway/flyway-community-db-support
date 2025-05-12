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

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywayDbUpgradeRequiredException;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

import java.math.BigInteger;
import java.sql.Connection;

public class IRISDatabase extends Database<IRISConnection> {
    public IRISDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected IRISConnection doGetConnection(Connection connection) {
        return new IRISConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
        MigrationVersion version = getVersion();
        if (Integer.parseInt(version.getMajorAsString()) < 2019) {
            throw new FlywayDbUpgradeRequiredException(new IRISDatabaseType(), version.toString(), "2019.1");
        }
    }

    @Override
    public boolean supportsDdlTransactions() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String schemaName = table.getSchema().getName();
        String tableName = table.getName();

        return "CREATE TABLE \"" + schemaName + "\".\"" + tableName + "\" (\n" +
                "  \"installed_rank\" INTEGER NOT NULL,\n" +
                "  \"version\" VARCHAR(50),\n" +
                "  \"description\" VARCHAR(200) NOT NULL,\n" +
                "  \"type\" VARCHAR(20) NOT NULL,\n" +
                "  \"script\" VARCHAR(1000) NOT NULL,\n" +
                "  \"checksum\" INTEGER,\n" +
                "  \"installed_by\" VARCHAR(100) NOT NULL,\n" +
                "  \"installed_on\" TIMESTAMP NOT NULL DEFAULT getdate(),\n" +
                "  \"execution_time\" INTEGER NOT NULL,\n" +
                "  \"success\" BIT NOT NULL\n" +
                ");\n" +
                "ALTER TABLE \"" + schemaName + "\".\"" + tableName + "\" ADD CONSTRAINT \"" + tableName + "_pk\" PRIMARY KEY (\"installed_rank\");";
    }
}
