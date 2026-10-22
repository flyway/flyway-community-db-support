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
package org.flywaydb.community.database.kingbaseoracle;

import org.flywaydb.community.database.KingbaseDatabaseExtension;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.extensibility.Tier;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.license.FlywayEditionUpgradeRequiredException;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.database.postgresql.PostgreSQLDatabaseType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class KingbaseOracleDatabaseType extends PostgreSQLDatabaseType implements CommunityDatabaseType {
    @Override
    public String getName() {
        return "Kingbase";
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        if (url.startsWith("jdbc-secretsmanager:kingbase8:")) {


            throw new FlywayEditionUpgradeRequiredException(Tier.ENTERPRISE, (Tier) null, "jdbc-secretsmanager");

        }
        return url.startsWith("jdbc:kingbase8:") || url.startsWith("jdbc:p6spy:kingbase8:");
    }

    @Override
    public int getPriority() {
        // Should be checked before plain PostgreSQL
        return 1;
    }

//    @Override
//    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
//        // The YB is what distinguishes Yugabyte
//        return databaseProductName.startsWith("Kingbase");
//    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        if (!databaseProductName.contains("Kingbase")) {
            return false;  // 先确保是 Kingbase 数据库
        }

        boolean result = checkDatabaseMode(connection, "oracle");
//        System.out.println("Checking Kingbase Oracle mode: " + result + ", databaseProductName: " + databaseProductName);
        return result;
    }

    private boolean checkDatabaseMode(Connection connection, String expectedMode) {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DATABASE_MODE;")) {

            if (rs.next()) {
                String mode = rs.getString(1).trim().toLowerCase();
                return expectedMode.equals(mode);
            }        } catch (SQLException e) {
            throw new FlywayException("Failed to determine Kingbase database mode", e);
        }
        return false;
    }


    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new KingbaseOracleDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new KingbaseOracleParser(configuration, parsingContext);
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return KingbaseDatabaseExtension.readVersion();
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

        if (url.startsWith("jdbc:p6spy:kingbase8:")) {
            return "com.p6spy.engine.spy.P6SpyDriver";
        }
        return "com.kingbase8.Driver";
    }

}
