/*-
 * ========================LICENSE_START=================================
 * flyway-database-polardb
 * ========================================================================
 * Copyright (C) 2010 - 2026 Red Gate Software Ltd
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
package org.flywaydb.community.database.polardb;

import org.flywaydb.community.database.PolarDBDatabaseExtension;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.database.postgresql.PostgreSQLDatabaseType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

public final class PolarDBDatabaseType
        extends PostgreSQLDatabaseType
        implements CommunityDatabaseType {
    private static final String POLARDB_JDBC_PREFIX = "jdbc:polardb:";
    private static final String POLARDB_DRIVER = "com.aliyun.polardb2.Driver";
    private static final String COMPATIBILITY_MODE_SQL =
            "SELECT current_setting('polar_compatibility_mode', true)";
    private static final Pattern FORCE_DRIVER_TYPE =
            Pattern.compile(
                    "[?&]forceDriverType=true(?:[&#]|$)",
                    Pattern.CASE_INSENSITIVE);

    @Override
    public String getName() {
        return "PolarDB";
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith(POLARDB_JDBC_PREFIX)
                || super.handlesJDBCUrl(url);
    }

    @Override
    public int getPriority() {
        // Inspect PolarDB before derived and generic PostgreSQL support.
        return 2;
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(
            String databaseProductName,
            String databaseProductVersion,
            Connection connection) {
        if (!databaseProductName.startsWith("PostgreSQL")) {
            return false;
        }

        try {
            String compatibilityMode =
                    readCompatibilityMode(connection);
            return "pg".equalsIgnoreCase(compatibilityMode)
                    || "ora".equalsIgnoreCase(compatibilityMode);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        if (url.startsWith(POLARDB_JDBC_PREFIX)
                || forcesPolarDBDriver(url)) {
            return POLARDB_DRIVER;
        }
        return super.getDriverClass(url, classLoader);
    }

    @Override
    public Parser createParser(
            Configuration configuration,
            ResourceProvider resourceProvider,
            ParsingContext parsingContext) {
        String compatibilityMode =
                readCompatibilityMode(parsingContext);
        return new PolarDBOracleParser(
                configuration,
                parsingContext,
                usesNestedBlockComments(compatibilityMode));
    }

    @Override
    public String getPluginVersion(Configuration configuration) {
        return PolarDBDatabaseExtension.readVersion();
    }

    private boolean forcesPolarDBDriver(String url) {
        return FORCE_DRIVER_TYPE.matcher(url).find();
    }

    private String readCompatibilityMode(
            ParsingContext parsingContext) {
        if (parsingContext.getDatabase() == null) {
            throw new FlywayException(
                    "Unable to determine the PolarDB compatibility mode: "
                            + "database context is not initialized");
        }

        try {
            return readCompatibilityMode(
                    parsingContext
                            .getDatabase()
                            .getMainConnection()
                            .getJdbcConnection());
        } catch (SQLException e) {
            throw new FlywayException(
                    "Unable to determine the PolarDB compatibility mode",
                    e);
        }
    }

    private static String readCompatibilityMode(
            Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet =
                        statement.executeQuery(COMPATIBILITY_MODE_SQL)) {
            return resultSet.next() ? resultSet.getString(1) : null;
        }
    }

    static boolean usesNestedBlockComments(
            String compatibilityMode) {
        if ("pg".equalsIgnoreCase(compatibilityMode)) {
            return true;
        }
        if ("ora".equalsIgnoreCase(compatibilityMode)) {
            return false;
        }
        throw new FlywayException(
                "Unsupported PolarDB compatibility mode: "
                        + compatibilityMode);
    }
}
