/*-
 * ========================LICENSE_START=================================
 * flyway-database-clickhouse
 * ========================================================================
 * Copyright (C) 2010 - 2024 Red Gate Software Ltd
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
/*
 * Copyright (C) Red Gate Software Ltd 2010-2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.community.database.clickhouse;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;

public class ClickHouseDatabase extends Database<ClickHouseConnection> {

    private ClickHouseConnection systemConnection;

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    public ClickHouseDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    public String getClusterName() {
        return configuration.getPluginRegister().getPlugin(ClickHouseConfigurationExtension.class).getClusterName();
    }

    public String getZookeeperPath() {
        return configuration.getPluginRegister().getPlugin(ClickHouseConfigurationExtension.class).getZookeeperPath();
    }

    public ClickHouseConnection getSystemConnection() {
        // Queries on system.XX fail with "Code: 81. DB::Exception: Database the_database doesn't exist. (UNKNOWN_DATABASE) (version 23.7.1.2470 (official build))"
        // in case the current catalog (database) is not yet created.
        // For this reason, we switch to an existing DB before execution. The database might not have been created yet, so we cannot reliably switch back the Schema.
        //  * mainConnection cannot be used, as this would change the location of the schema history table.
        //  * jdbcTemplate cannot be used, as this would change the location of the new tables.
        // We had to introduce a separate connection, reserved to system database access.
        if (systemConnection == null) {
            Connection connection = jdbcConnectionFactory.openConnection();
            try {
                systemConnection = doGetConnection(connection);
                systemConnection.doChangeCurrentSchemaOrSearchPathTo("system");
            } catch (SQLException e) {
                throw new FlywaySqlException("Unable to switch connection to read-only", e);
            }
        }
        return systemConnection;
    }

    @Override
    protected ClickHouseConnection doGetConnection(Connection connection) {
        return new ClickHouseConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public boolean supportsMultiStatementTransactions() {
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
        return true;
    }

    @Override
    public void close() {
        if (systemConnection != null) {
            systemConnection.close();
        }

        super.close();
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String clusterName = getClusterName();
        boolean isClustered = StringUtils.hasText(clusterName);

        String script = "CREATE TABLE IF NOT EXISTS " + table + (isClustered ? (" ON CLUSTER " + clusterName) : "") + "(" +
                "    installed_rank Int32," +
                "    version Nullable(String)," +
                "    description String," +
                "    type String," +
                "    script String," +
                "    checksum Nullable(Int32)," +
                "    installed_by String," +
                "    installed_on DateTime DEFAULT now()," +
                "    execution_time Int32," +
                "    success Bool" +
                ")";

        String engine;

        if (isClustered) {
            engine = "ReplicatedMergeTree('" + getZookeeperPath() + "', '{replica}')";
        } else {
            engine = "MergeTree";
        }

        script += " ENGINE = " + engine +
                " PRIMARY KEY (script);";

        return script + (baseline ? getBaselineStatement(table) + ";" : "");
    }
}
