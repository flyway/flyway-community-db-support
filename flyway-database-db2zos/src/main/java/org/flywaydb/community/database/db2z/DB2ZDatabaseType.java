/*-
 * ========================LICENSE_START=================================
 * flyway-database-db2zos
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
 * Copyright (C) Red Gate Software Ltd 2010-2022
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
package org.flywaydb.community.database.db2z;

import java.sql.Connection;
import java.sql.Types;
import java.util.Properties;
import org.flywaydb.community.database.DB2ZDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.callback.CallbackExecutor;
import org.flywaydb.core.internal.database.DatabaseType;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.sqlscript.DefaultSqlScriptExecutor;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;

public class DB2ZDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {

    public String getName() {
        return "DB2 for z/OS";
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:db2:") || url.startsWith("jdbc:p6spy:db2:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        if (url.startsWith("jdbc:p6spy:db2:")) {
            return "com.p6spy.engine.spy.P6SpyDriver";
        }
        return "com.ibm.db2.jcc.DB2Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        return databaseProductName.startsWith("DB2") && databaseProductVersion.startsWith("DSN");
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new DB2ZDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new DB2ZParser(configuration, parsingContext);
    }

    @Override
    public void setDefaultConnectionProps(String url, Properties props, ClassLoader classLoader) {
        props.put("clientProgramName", APPLICATION_NAME);
        props.put("retrieveMessagesFromServerOnGetMessage", "true");
    }

    @Override
    public SqlScriptExecutorFactory createSqlScriptExecutorFactory(final JdbcConnectionFactory jdbcConnectionFactory,
        final CallbackExecutor callbackExecutor,
        final StatementInterceptor statementInterceptor) {
        boolean supportsBatch = false;




        final boolean finalSupportsBatch = supportsBatch;
        final DatabaseType thisRef = this;

        return (connection, undo, batch, outputQueryResults) -> new DefaultSqlScriptExecutor(new DB2ZJdbcTemplate(connection, thisRef),
            callbackExecutor, undo, finalSupportsBatch && batch, outputQueryResults, statementInterceptor);
    }

    @Override
    public int getPriority() {
        // DB2/zOS needs to be checked in advance of DB2
        return 1;
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return DB2ZDatabaseExtension.readVersion();
    }
}
