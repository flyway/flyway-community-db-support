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

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

import java.sql.Connection;

/**
 * StarRocks database.
 * 
 * Extends the base Database class directly instead of MySQLDatabase
 * because StarRocks doesn't support many MySQL-specific features and system variables.
 */
public class StarRocksDatabase extends Database<StarRocksConnection> {

    @Override
    public String doQuote(String identifier) {
        // StarRocks uses backticks for quoting identifiers, like MySQL
        return "`" + identifier + "`";
    }

    public StarRocksDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected StarRocksConnection doGetConnection(Connection connection) {
        return new StarRocksConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
        // StarRocks 2.0+ is supported
        ensureDatabaseIsRecentEnough("2.0");
        recommendFlywayUpgradeIfNecessary("3.0");
    }

    @Override
    public boolean supportsDdlTransactions() {
        // StarRocks does not support DDL transactions
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
    public String getRawCreateScript(Table table, boolean baseline) {
        // StarRocks requires a specific table engine with PRIMARY KEY and DISTRIBUTED BY
        return "CREATE TABLE " + table + " (\n" +
                "    `installed_rank` INT NOT NULL,\n" +
                "    `version` VARCHAR(50),\n" +
                "    `description` VARCHAR(200) NOT NULL,\n" +
                "    `type` VARCHAR(20) NOT NULL,\n" +
                "    `script` VARCHAR(1000) NOT NULL,\n" +
                "    `checksum` INT,\n" +
                "    `installed_by` VARCHAR(100) NOT NULL,\n" +
                "    `installed_on` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "    `execution_time` INT NOT NULL,\n" +
                "    `success` BOOLEAN NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`installed_rank`)\n" +
                "DISTRIBUTED BY HASH(`installed_rank`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");\n" +
                (baseline ? getBaselineStatement(table) + ";\n" : "");
    }

    @Override
    public String getSelectStatement(Table table) {
        return "SELECT " + quote("installed_rank") + "," + quote("version") + "," + quote("description") + "," +
                quote("type") + "," + quote("script") + "," + quote("checksum") + "," + quote("installed_by") + "," +
                quote("installed_on") + "," + quote("execution_time") + "," + quote("success") +
                " FROM " + table +
                " WHERE " + quote("installed_rank") + " > ?" +
                " ORDER BY " + quote("installed_rank");
    }

    @Override
    public String getInsertStatement(Table table) {
        return "INSERT INTO " + table +
                " (" + quote("installed_rank") + ", " + quote("version") + ", " + quote("description") + ", " +
                quote("type") + ", " + quote("script") + ", " + quote("checksum") + ", " + quote("installed_by") + ", " +
                quote("installed_on") + ", " + quote("execution_time") + ", " + quote("success") + ")" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)";
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }
}
