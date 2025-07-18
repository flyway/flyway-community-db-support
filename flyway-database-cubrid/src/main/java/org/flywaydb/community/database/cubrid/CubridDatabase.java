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

import java.sql.Connection;
import lombok.CustomLog;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

@CustomLog
public class CubridDatabase extends Database<CubridConnection> {

    public CubridDatabase(Configuration configuration,
        JdbcConnectionFactory jdbcConnectionFactory,
        StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected CubridConnection doGetConnection(Connection connection) {
        return new CubridConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
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
        return "CREATE TABLE IF NOT EXISTS " + table + " (\n" +
            "    \"installed_rank\" INT NOT NULL PRIMARY KEY,\n" +
            "    \"version\" VARCHAR(50),\n" +
            "    \"description\" VARCHAR(200) NOT NULL,\n" +
            "    \"type\" VARCHAR(20) NOT NULL,\n" +
            "    \"script\" VARCHAR(1000) NOT NULL,\n" +
            "    \"checksum\" INT,\n" +
            "    \"installed_by\" VARCHAR(100) NOT NULL,\n" +
            "    \"installed_on\" TIMESTAMP NOT NULL DEFAULT NOW(),\n" +
            "    \"execution_time\" INT NOT NULL,\n" +
            "    \"success\" INT NOT NULL\n" +
            ");\n"
            + (baseline ? getBaselineStatement(table) + ";\n" : "")
            + "CREATE INDEX " + table.getName() + "_s_idx ON " + table + " (\"success\");";
    }

    @Override
    public boolean useSingleConnection() {
        return false;
    }
}
