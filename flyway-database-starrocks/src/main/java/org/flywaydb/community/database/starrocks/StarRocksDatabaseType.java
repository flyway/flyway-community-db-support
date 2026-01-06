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

import org.flywaydb.community.database.StarRocksDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

/**
 * StarRocks database type.
 * 
 * StarRocks uses MySQL protocol but is a separate OLAP database with different capabilities.
 * This implementation does not extend MySQLDatabaseType to avoid inheriting MySQL-specific
 * behavior that StarRocks doesn't support.
 */
public class StarRocksDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {
    
    @Override
    public String getName() {
        return "StarRocks";
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public int getPriority() {
        // StarRocks needs to be checked before MySQL since it uses the same JDBC URL prefix
        return 1;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        // StarRocks uses MySQL protocol, so the product name contains "MySQL"
        if (!databaseProductName.contains("MySQL")) {
            return false;
        }
        
        // Detect StarRocks by trying a StarRocks-specific command: SHOW FRONTENDS
        // This command only exists in StarRocks and will fail on regular MySQL
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SHOW FRONTENDS");
            // If the query succeeds, this is StarRocks
            rs.close();
            return true;
        } catch (Exception e) {
            // If the query fails, this is not StarRocks (probably regular MySQL)
            return false;
        }
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new StarRocksDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new StarRocksParser(configuration, parsingContext);
    }

    @Override
    public boolean detectUserRequiredByUrl(String url) {
        return !url.contains("user=");
    }

    @Override
    public boolean detectPasswordRequiredByUrl(String url) {
        return !url.contains("password=");
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return StarRocksDatabaseExtension.readVersion();
    }
}
