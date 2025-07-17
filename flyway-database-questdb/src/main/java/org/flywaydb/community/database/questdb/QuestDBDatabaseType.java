/*-
 * ========================LICENSE_START=================================
 * flyway-database-questdb
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
package org.flywaydb.community.database.questdb;

import lombok.CustomLog;
import org.flywaydb.community.database.QuestDBDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@CustomLog
public class QuestDBDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {
    @Override
    public String getName() {
        return "QuestDB";
    }

    @Override
    public int getNullType() {
        return 0;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return "org.postgresql.Driver";
    }

    @Override
    public String getBackupDriverClass(String url, ClassLoader classLoader) {
        return "org.postgresql.Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        if (databaseProductName.startsWith("PostgreSQL")) {
            try (PreparedStatement stmt = connection.prepareStatement("select version()");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    final String version = rs.getString(1);
                    return version.endsWith("QuestDB");
                }
            } catch (SQLException e) {
                LOG.error("Could not query catalog version from server");
                return false;
            }
        }
        return false;
    }

    @Override
    public int getPriority() {
        return 1;
    }

    @Override
    public QuestDBDatabase createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new QuestDBDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new QuestDBParser(configuration, parsingContext, 3);
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
        return QuestDBDatabaseExtension.readVersion();
    }
}
