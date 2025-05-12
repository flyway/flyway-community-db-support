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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.flywaydb.community.database.IRISDatabaseExtension;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class IRISDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {
    @Override
    public String getName() {
        return "InterSystems IRIS Data Platform";
    }

    @Override
    public int getNullType() {
        return Types.NULL;
    }

    @Override
    public boolean handlesJDBCUrl(final String url) {
    return url.startsWith("jdbc:IRIS:");
    }

    @Override
    public String getDriverClass(final String url, final ClassLoader classLoader) {
        return "com.intersystems.jdbc.IRISDriver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(final String databaseProductName,
                                                        final String databaseProductVersion,
                                                        final Connection connection) {
        int majorVersion;
        try {
            majorVersion = connection.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new FlywayException(String.format("Unable to load database major version for %s-%s", databaseProductName, databaseProductVersion), e);
        }
        return databaseProductName.startsWith("InterSystems IRIS") && majorVersion >= 2019;
    }

    @Override
    public IRISDatabase createDatabase(final Configuration configuration,
                                       final JdbcConnectionFactory jdbcConnectionFactory,
                                       final StatementInterceptor statementInterceptor) {
        return new IRISDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(final Configuration configuration,
                               final ResourceProvider resourceProvider,
                               final ParsingContext parsingContext) {
        return new IRISParser(configuration, parsingContext, 3);
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return IRISDatabaseExtension.readVersion();
    }
}
