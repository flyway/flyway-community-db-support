/*
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
 *
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
 */
package org.flywaydb.community.database.cubrid;

import java.sql.Connection;
import java.sql.Types;
import org.flywaydb.community.database.CubridDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class CubridDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {

    @Override
    public String getName() {
        return "CUBRID";
    }

    @Override
    public int getNullType() {
        return Types.NULL;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:cubrid:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return "cubrid.jdbc.driver.CUBRIDDriver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName,
        String databaseProductVersion,
        Connection connection) {
        return databaseProductName != null && databaseProductName.trim().toLowerCase()
            .contains("cubrid");
    }


    @Override
    public Database<CubridConnection> createDatabase(Configuration configuration,
        JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new CubridDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider,
        ParsingContext parsingContext) {
        return new CubridParser(configuration, parsingContext);
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return CubridDatabaseExtension.readVersion();
    }
}
