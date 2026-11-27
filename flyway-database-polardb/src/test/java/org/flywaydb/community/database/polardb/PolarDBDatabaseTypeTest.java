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
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.extensibility.Plugin;
import org.flywaydb.core.internal.database.DatabaseTypeRegister;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;

class PolarDBDatabaseTypeTest {
    private final PolarDBDatabaseType databaseType =
            new PolarDBDatabaseType();

    @Test
    void handlesPolarDBAndPostgreSQLUrls() {
        assertThat(databaseType.handlesJDBCUrl(
                "jdbc:polardb://localhost:1521/testdb")).isTrue();
        assertThat(databaseType.handlesJDBCUrl(
                "jdbc:postgresql://localhost:1521/testdb"
                        + "?ssl=true&FoRcEdRiVeRtYpE=true"))
                .isTrue();
        assertThat(databaseType.handlesJDBCUrl(
                "jdbc:postgresql://localhost:5432/testdb")).isTrue();
        assertThat(databaseType.handlesJDBCUrl(
                "jdbc:mysql://localhost:3306/testdb"
                        + "?forceDriverType=true"))
                .isFalse();
    }

    @Test
    void selectsPolarDBDriverForSupportedUrls() {
        assertThat(databaseType.getDriverClass(
                "jdbc:polardb://localhost:1521/testdb",
                getClass().getClassLoader()))
                .isEqualTo("com.aliyun.polardb2.Driver");
        assertThat(databaseType.getDriverClass(
                "jdbc:postgresql://localhost:1521/testdb"
                        + "?forceDriverType=true",
                getClass().getClassLoader()))
                .isEqualTo("com.aliyun.polardb2.Driver");
    }

    @Test
    void leavesPlainPostgreSQLUrlsToPostgreSQLSupport() {
        assertThat(databaseType.getDriverClass(
                "jdbc:postgresql://localhost:5432/testdb",
                getClass().getClassLoader()))
                .isEqualTo("org.postgresql.Driver");
    }

    @Test
    void usesPostgreSQLDriverForOtherForceValues() {
        for (String forceDriverType : List.of("false", "pg", "truex")) {
            assertThat(databaseType.getDriverClass(
                    "jdbc:postgresql://localhost:1521/testdb"
                            + "?forceDriverType="
                            + forceDriverType,
                    getClass().getClassLoader()))
                    .isEqualTo("org.postgresql.Driver");
        }
    }

    @Test
    void detectsPolarDBCompatibilityModes() {
        for (String mode : List.of("pg", "ora")) {
            assertThat(databaseType.handlesDatabaseProductNameAndVersion(
                    "PostgreSQL",
                    "14.21",
                    connectionReturningCompatibilityMode(mode)))
                    .isTrue();
        }
    }

    @Test
    void rejectsUnknownCompatibilityModes() {
        for (String mode : List.of("", "mysql")) {
            assertThat(databaseType
                    .handlesDatabaseProductNameAndVersion(
                            "PostgreSQL",
                            "14.21",
                            connectionReturningCompatibilityMode(mode)))
                    .isFalse();
        }
    }

    @Test
    void rejectsMissingCompatibilityMode() {
        assertThat(databaseType.handlesDatabaseProductNameAndVersion(
                "PostgreSQL",
                "14.21",
                connectionReturningCompatibilityMode(null)))
                .isFalse();
    }

    @Test
    void rejectsOtherDatabaseProducts() {
        assertThat(databaseType.handlesDatabaseProductNameAndVersion(
                "MySQL",
                "8.0",
                connectionReturningCompatibilityMode("ora")))
                .isFalse();
    }

    @Test
    void rejectsConnectionsWhenCompatibilityModeCannotBeRead() {
        assertThat(databaseType.handlesDatabaseProductNameAndVersion(
                "PostgreSQL",
                "14.21",
                connectionThrowingSQLException()))
                .isFalse();
    }

    @Test
    void selectsCommentStyleFromCompatibilityMode() {
        assertThat(PolarDBDatabaseType
                .usesNestedBlockComments("pg")).isTrue();
        assertThat(PolarDBDatabaseType
                .usesNestedBlockComments("ora")).isFalse();
    }

    @Test
    void exposesDatabaseTypeThroughServiceLoader() {
        List<Class<? extends Plugin>> pluginTypes =
                ServiceLoader.load(Plugin.class)
                        .stream()
                        .map(ServiceLoader.Provider::type)
                        .toList();

        assertThat(pluginTypes).contains(PolarDBDatabaseType.class);
    }

    @Test
    void exposesPluginVersion() {
        assertThat(PolarDBDatabaseExtension.readVersion())
                .matches("[0-9]+\\.[0-9]+\\.[0-9]+"
                        + "(?:[-.][0-9A-Za-z.-]+)?");
    }

    @Test
    void databaseTypeRegisterSelectsPolarDBForBothModes() {
        FluentConfiguration configuration = new FluentConfiguration();

        for (String mode : List.of("pg", "ora")) {
            assertThat(DatabaseTypeRegister.getDatabaseTypeForConnection(
                    connectionReturningCompatibilityMode(mode),
                    configuration))
                    .isExactlyInstanceOf(PolarDBDatabaseType.class);
        }
    }

    private Connection connectionReturningCompatibilityMode(String mode) {
        DatabaseMetaData metadata =
                (DatabaseMetaData) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {DatabaseMetaData.class},
                        (proxy, method, args) -> {
                            if ("getDatabaseProductName"
                                    .equals(method.getName())) {
                                return "PostgreSQL";
                            }
                            if ("getDatabaseProductVersion"
                                    .equals(method.getName())) {
                                return "14.21";
                            }
                            if ("getDatabaseMajorVersion"
                                    .equals(method.getName())) {
                                return 14;
                            }
                            if ("getDatabaseMinorVersion"
                                    .equals(method.getName())) {
                                return 21;
                            }
                            throw new UnsupportedOperationException(
                                    method.getName());
                        });

        ResultSet resultSet = (ResultSet) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    if ("next".equals(method.getName())) {
                        return true;
                    }
                    if ("getString".equals(method.getName())) {
                        return mode;
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    throw new UnsupportedOperationException(
                            method.getName());
                });

        Statement statement = (Statement) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Statement.class},
                (proxy, method, args) -> {
                    if ("executeQuery".equals(method.getName())) {
                        assertThat(args[0]).isEqualTo(
                                "SELECT current_setting("
                                        + "'polar_compatibility_mode', true)");
                        return resultSet;
                    }
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    throw new UnsupportedOperationException(
                            method.getName());
                });

        return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("createStatement".equals(method.getName())) {
                        return statement;
                    }
                    if ("getMetaData".equals(method.getName())) {
                        return metadata;
                    }
                    throw new UnsupportedOperationException(
                            method.getName());
                });
    }

    private Connection connectionThrowingSQLException() {
        return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("createStatement".equals(method.getName())) {
                        throw new SQLException(
                                "compatibility mode unavailable");
                    }
                    throw new UnsupportedOperationException(
                            method.getName());
                });
    }
}
