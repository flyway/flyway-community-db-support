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

import org.flywaydb.community.database.YugabyteDBDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.database.postgresql.PostgreSQLDatabaseType;

import java.sql.Connection;
import java.util.regex.Pattern;

public class YugabyteDBDatabaseType extends PostgreSQLDatabaseType implements CommunityDatabaseType {
    @Override
    public String getName() {
        return "YugabyteDB";
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:yugabytedb:") || url.startsWith("jdbc:postgresql:") || url.startsWith("jdbc:p6spy:postgresql:");
    }

    @Override
    public int getPriority() {
        // Should be checked before plain PostgreSQL
        return 1;
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        // The YB is what distinguishes Yugabyte
        return databaseProductName.startsWith("PostgreSQL")
                && Pattern.matches("PostgreSQL\\s\\d{1,2}(\\.\\d{1,2})?-YB-\\d{1,2}(\\.\\d{1,2})?.*", getSelectVersionOutput(connection));
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new YugabyteDBDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new YugabyteDBParser(configuration, parsingContext);
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return YugabyteDBDatabaseExtension.readVersion();
    }

    /**
     * Returns the YugabyteDB Smart driver classname if the smart driver is
     * being used. The plugin will work with the Postgresql JDBC driver also
     * since the url in that case would start with 'jdbc:postgresql' which would
     * return the PG JDBC driver class name.
     * @param url
     * @param classLoader
     * @return "com.yugabyte.Driver" if url starts with "jdbc:yugabytedb:"
     */
    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return url.startsWith("jdbc:yugabytedb:") ? "com.yugabyte.Driver" : super.getDriverClass(url, classLoader);
    }
}
