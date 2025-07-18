/*-
 * ========================LICENSE_START=================================
 * flyway-database-tidb
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

package org.flywaydb.community.database.mysql.tidb;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.database.mysql.MySQLConnection;
import org.flywaydb.database.mysql.MySQLDatabase;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

import java.sql.Connection;

public class TiDBDatabase extends MySQLDatabase {

    public TiDBDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected MySQLConnection doGetConnection(Connection connection) {
        return new TiDBConnection(this, connection);
    }

    @Override
    protected boolean isCreateTableAsSelectAllowed() {return false;}

    @Override
    public void ensureSupported(Configuration configuration) {
        ensureDatabaseIsRecentEnough("5.0");
        recommendFlywayUpgradeIfNecessary("5.0");
    }
}
