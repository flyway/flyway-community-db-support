/*-
 * ========================LICENSE_START=================================
 * flyway-database-yugabytedb
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
package org.flywaydb.community.database.kingbasesqlserver;

import lombok.CustomLog;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.database.postgresql.PostgreSQLDatabase;

import java.sql.Connection;
import java.sql.SQLException;


@CustomLog
public class KingbaseSQLServerDatabase extends PostgreSQLDatabase {

    public static final String LOCK_TABLE_NAME = "YB_FLYWAY_LOCK_TABLE";
    /**
     * This table is used to enforce locking through SELECT ... FOR UPDATE on a
     * token row inserted in this table. The token row is inserted with the name
     * of the Flyway's migration history table as a token for simplicity.
     */
//    private static final String CREATE_LOCK_TABLE_DDL = "CREATE TABLE IF NOT EXISTS " + LOCK_TABLE_NAME + " (table_name varchar PRIMARY KEY, locked bool)";
    private static final String CREATE_LOCK_TABLE_DDL =
//            "CREATE TABLE IF NOT EXISTS " + LOCK_TABLE_NAME +
//                    " (table_name VARCHAR(255) PRIMARY KEY, locked BOOLEAN NOT NULL DEFAULT FALSE)";
            "CREATE TABLE IF NOT EXISTS " + LOCK_TABLE_NAME +
                    " (table_name VARCHAR(255) PRIMARY KEY, locked BIT NOT NULL DEFAULT 0)";//将locked的boolean类型修改为bit类型
    public KingbaseSQLServerDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
        createLockTable();
    }

    @Override
    protected KingbaseSQLServerConnection doGetConnection(Connection connection) {
        return new KingbaseSQLServerConnection(this, connection);
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
    public void ensureSupported(Configuration configuration) {
        // Checks the Postgres version
        ensureDatabaseIsRecentEnough("11.2");
    }


    @Override
    public boolean supportsDdlTransactions() {
        return true;
    }

// 修改建表语句  在一个书事务之后加GO
@Override
public String getRawCreateScript(Table table, boolean baseline) {
    return "CREATE TABLE " + table + " (\n" +
            "    [installed_rank] INT NOT NULL,\n" +
            "    [version] NVARCHAR(50),\n" +
            "    [description] NVARCHAR(200),\n" +
            "    [type] NVARCHAR(20) NOT NULL,\n" +
            "    [script] NVARCHAR(1000) NOT NULL,\n" +
            "    [checksum] INT,\n" +
            "    [installed_by] NVARCHAR(100) NOT NULL,\n" +
            "    [installed_on] DATETIME NOT NULL DEFAULT GETDATE(),\n" +
            "    [execution_time] INT NOT NULL,\n" +
            "    [success] BIT NOT NULL\n" +
            ")" +  ";\nGO\n" +  // 每个 CREATE TABLE 后加 GO
            (baseline ? getBaselineStatement(table) + ";\nGO\n" : "") + // 如果 baseline 需要执行，也加 GO
            "ALTER TABLE " + table + " ADD CONSTRAINT [" + table.getName() + "_pk] PRIMARY KEY ([installed_rank]);\nGO\n" + // ALTER TABLE 后加 GO
            "CREATE INDEX [" + table.getName() + "_s_idx] ON " + table + " ([success]);\nGO\n"; // CREATE INDEX 后加 GO
}

    //新增
    @Override
    public String getOpenQuote() { return "["; }
    @Override
    public String getCloseQuote() { return "]"; }
    @Override
    public String getEscapedQuote() { return "]]"; }

    @Override
    public Delimiter getDefaultDelimiter() {
        return Delimiter.GO;
    }

    @Override
    public boolean useSingleConnection() {
        return true;
    }

    private void createLockTable() {
        try {
            jdbcTemplate.execute(CREATE_LOCK_TABLE_DDL);
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to initialize the lock table", e);
        }
    }
}


