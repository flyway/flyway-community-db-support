/*-
 * ========================LICENSE_START=================================
 * flyway-database-doris
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

package org.flywaydb.community.database.doris;

import org.flywaydb.community.database.DorisDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.util.ClassUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class DorisDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {

    @Override
    public String getName() {
        return "Doris";
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public int getPriority() {
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
    public String getBackupDriverClass(String url, ClassLoader classLoader) {
        if (ClassUtils.isPresent("com.mysql.jdbc.Driver", classLoader)) {
            return "com.mysql.jdbc.Driver";
        }
        return null;
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        if (!databaseProductName.contains("MySQL")) {
            return false;
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT @@version_comment");
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                String versionComment = resultSet.getString(1);
                if (versionComment != null) {
                    String lower = versionComment.toLowerCase();
                    return lower.contains("doris") || lower.contains("selectdb") || lower.contains("velodb");
                }
            }
        } catch (SQLException ignored) {
        }
        return false;
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new DorisDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new DorisParser(configuration, parsingContext);
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return DorisDatabaseExtension.readVersion();
    }
}
