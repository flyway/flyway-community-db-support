/*-
 * ========================LICENSE_START=================================
 * flyway-database-doris
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
package org.flywaydb.community.database.doris;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DorisTest {
    private static final DockerImageName DORIS_IMAGE = DockerImageName.parse("apache/doris:doris-all-in-one-2.1.0");
    private static final int FE_QUERY_PORT = 9030;
    private static final int FE_HTTP_PORT = 8030;
    private static final int BE_HTTP_PORT = 8040;
    private static final long ASSERT_QUERY_TIMEOUT_MS = 60_000L;
    private static final long DORIS_READY_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);
    private static final String DATABASE = "test_db";
    private static final String LOCATION = "doris_migration";
    private static final String USER = "root";
    private static final String PWD = "";

    @ClassRule
    public static final GenericContainer<?> DORIS = new GenericContainer<>(DORIS_IMAGE)
            .withExposedPorts(FE_QUERY_PORT, FE_HTTP_PORT, BE_HTTP_PORT)
            .withStartupTimeout(java.time.Duration.ofMinutes(5))
            .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));

    @BeforeClass
    public static void prepareDoris() throws SQLException {
        waitForDorisReady();
        try (Connection connection = DriverManager.getConnection(bootstrapJdbcUrl(), USER, PWD);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
        }
    }

    @Test
    public void testMigration1_CreateTable() throws SQLException {
        assertMigration(
                "1",
                "SELECT table_name FROM information_schema.tables WHERE table_schema='" + DATABASE + "' ORDER BY table_name",
                "table_name\n" +
                        "flyway_schema_history\n" +
                        "trades\n"
        );
    }

    @Test
    public void testMigration2_InsertTrades() throws SQLException {
        assertMigration(
                "2",
                "SELECT id, symbol, price FROM trades ORDER BY id",
                "id\tsymbol\tprice\n" +
                        "1\tAAPL\t150.00\n" +
                        "2\tGOOG\t2800.50\n" +
                        "3\tMSFT\t410.25\n"
        );
    }

    @Test
    public void testMigration3_CreateView() throws SQLException {
        assertMigration(
                "3",
                "SELECT table_name FROM information_schema.views WHERE table_schema='" + DATABASE + "'",
                "table_name\n" +
                        "recent_trades\n"
        );
    }

    @Test
    public void testMigration4_DropView() throws SQLException {
        assertMigration(
                "4",
                "SELECT COUNT(*) AS view_count FROM information_schema.views WHERE table_schema='" + DATABASE + "'",
                "view_count\n" +
                        "0\n"
        );
    }

    @Test
    public void testMigration5_DropTable() throws SQLException {
        assertMigration(
                "5",
                "SELECT table_name FROM information_schema.tables WHERE table_schema='" + DATABASE + "' AND table_name='trades'",
                "table_name\n"
        );
    }

    private void assertMigration(String version, String query, String expectedResult) throws SQLException {
        final String jdbcUrl = jdbcUrl();

        final Flyway flyway = flyway(jdbcUrl, version);
        final MigrateResult migrateResult = flyway.migrate();
        assertEquals(0, migrateResult.getFailedMigrations().size());

        assertQuery(jdbcUrl, query, expectedResult);
    }

    private static String jdbcUrl() {
        return "jdbc:mysql://" + DORIS.getHost() + ":" + DORIS.getMappedPort(FE_QUERY_PORT)
                + "/" + DATABASE
                + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    private static String bootstrapJdbcUrl() {
        return "jdbc:mysql://" + DORIS.getHost() + ":" + DORIS.getMappedPort(FE_QUERY_PORT)
                + "/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    private static Flyway flyway(String jdbcUrl, String version) {
        return Flyway
                .configure()
                .locations(LOCATION)
                .dataSource(jdbcUrl, USER, PWD)
                .target(version)
                .load();
    }

    private static void waitForDorisReady() {
        final long deadline = System.currentTimeMillis() + DORIS_READY_TIMEOUT_MS;
        SQLException lastException = null;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = DriverManager.getConnection(bootstrapJdbcUrl(), USER, PWD);
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SHOW BACKENDS")) {
                while (resultSet.next()) {
                    if ("true".equalsIgnoreCase(resultSet.getString("Alive"))) {
                        return;
                    }
                }
                lastException = null;
            } catch (SQLException e) {
                lastException = e;
            }
            sleep(2000L);
        }
        throw new IllegalStateException("Doris BE did not become alive within "
                + DORIS_READY_TIMEOUT_MS + "ms", lastException);
    }

    private static void assertQuery(String jdbcUrl, String query, String expectedResult) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PWD)) {
            final long endTime = System.currentTimeMillis() + ASSERT_QUERY_TIMEOUT_MS;
            String actualResult;
            do {
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query)) {
                    actualResult = resultSetToString(resultSet);
                    if (expectedResult.equals(actualResult)) {
                        return;
                    }
                }
                sleep(500L);
            } while (System.currentTimeMillis() < endTime);
            assertEquals(expectedResult, actualResult);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String resultSetToString(ResultSet rs) throws SQLException {
        final StringBuilder sb = new StringBuilder();
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            sb.append(metaData.getColumnName(i));
            if (i < columnCount) {
                sb.append("\t");
            }
        }
        sb.append("\n");

        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                sb.append(rs.getString(i));
                if (i < columnCount) {
                    sb.append("\t");
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }
}
