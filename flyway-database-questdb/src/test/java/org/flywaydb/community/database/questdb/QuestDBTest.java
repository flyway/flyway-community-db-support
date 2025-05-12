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

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;

import static org.junit.Assert.*;

public class QuestDBTest {
    private static final DockerImageName QUESTDB_IMAGE = DockerImageName.parse("questdb/questdb:nightly");
    private static final int HTTP_PORT = 9000;
    private static final int PG_PORT = 8812;
    private static final String LOCATION = "classpath:questdb/migration";
    private static final String USER = "admin";
    private static final String PWD = "quest";

    @Rule
    public GenericContainer<?> questdb = new GenericContainer<>(QUESTDB_IMAGE)
            .withExposedPorts(HTTP_PORT, PG_PORT)
            .withLogConsumer(outputFrame -> System.out.println(outputFrame.getUtf8String()));

    @Test
    public void testMigration1_CreateTable() throws SQLException {
        assertMigration(
                "1",
                "select table_name, designatedTimestamp, partitionBy,walEnabled from tables()",
                "table_name\tdesignatedTimestamp\tpartitionBy\twalEnabled\n" +
                        "trades\tts\tDAY\tt\n" +
                        "flyway_schema_history\tinstalled_on\tDAY\tt\n"
        );
    }

    @Test
    public void testMigration2_InsertData() throws SQLException {
        assertMigration(
                "2",
                "trades",
                "instrument\tside\tqty\tprice\tts\n" +
                "SYM1\tBUY\t100.0\t12.56\t2025-05-09 00:01:00.000000\n" +
                "SYM2\tBUY\t120.0\t10.11\t2025-05-09 00:02:00.000000\n" +
                "SYM1\tSELL\t50.0\t12.44\t2025-05-09 00:03:00.000000\n"
        );
    }

    @Test
    public void testMigration3_RenameTable() throws SQLException {
        assertMigration(
                "3",
                "trades_table",
                "instrument\tside\tqty\tprice\tts\n" +
                "SYM1\tBUY\t100.0\t12.56\t2025-05-09 00:01:00.000000\n" +
                "SYM2\tBUY\t120.0\t10.11\t2025-05-09 00:02:00.000000\n" +
                "SYM1\tSELL\t50.0\t12.44\t2025-05-09 00:03:00.000000\n"
        );
    }

    @Test
    public void testMigration4_AddColumn() throws SQLException {
        assertMigration(
                "4",
                "show create table trades",
                "ddl\n" +
                        "CREATE TABLE 'trades' ( \n" +
                        "\tinstrument SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tside SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tqty DOUBLE,\n" +
                        "\tprice DOUBLE,\n" +
                        "\tts TIMESTAMP,\n" +
                        "\tvenue VARCHAR\n" +
                        ") timestamp(ts) PARTITION BY DAY WAL\n" +
                        "WITH maxUncommittedRows=500000, o3MaxLag=600000000us;\n"
        );
    }

    @Test
    public void testMigration5_AlterColumnType() throws SQLException {
        assertMigration(
                "5",
                "show create table trades",
                "ddl\n" +
                        "CREATE TABLE 'trades' ( \n" +
                        "\tinstrument SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tside SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tqty DOUBLE,\n" +
                        "\tprice DOUBLE,\n" +
                        "\tts TIMESTAMP,\n" +
                        "\tvenue SYMBOL CAPACITY 256 CACHE\n" +
                        ") timestamp(ts) PARTITION BY DAY WAL\n" +
                        "WITH maxUncommittedRows=500000, o3MaxLag=600000000us;\n"
        );
    }

    @Test
    public void testMigration6_DropColumn() throws SQLException {
        assertMigration(
                "6",
                "show create table trades",
                "ddl\n" +
                        "CREATE TABLE 'trades' ( \n" +
                        "\tinstrument SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tside SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tqty DOUBLE,\n" +
                        "\tprice DOUBLE,\n" +
                        "\tts TIMESTAMP\n" +
                        ") timestamp(ts) PARTITION BY DAY WAL\n" +
                        "WITH maxUncommittedRows=500000, o3MaxLag=600000000us;\n"
        );
    }

    @Test
    public void testMigration7_DropTable() throws SQLException {
        assertMigration(
                "7",
                "select table_name, designatedTimestamp, partitionBy,walEnabled from tables()",
                "table_name\tdesignatedTimestamp\tpartitionBy\twalEnabled\n" +
                        "flyway_schema_history\tinstalled_on\tDAY\tt\n"
        );
    }

    private void assertMigration(String version, String query, String expectedResult) throws SQLException {
        final String jdbcUrl = jdbcUrl();

        final Flyway flyway = flyway(jdbcUrl, version);
        final MigrateResult migrateResult = flyway.migrate();
        assertEquals(0, migrateResult.getFailedMigrations().size());

        assertQuery(jdbcUrl, query, expectedResult);
    }

    private String jdbcUrl() {
        final String host = questdb.getHost();
        final int port = questdb.getMappedPort(PG_PORT);
        return "jdbc:postgresql://" + host + ":" + port + "/default";
    }

    private static Flyway flyway(String jdbcUrl, String version) {
        return Flyway
                .configure()
                .locations(LOCATION)
                .dataSource(jdbcUrl, USER, PWD)
                .target(version)
                .load();
    }

    private static void assertQuery(String jdbcUrl, String query, String expectedResult) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PWD)) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {
                assertEquals(expectedResult, resultSetToString(resultSet));
            }
        }
    }

    private static String resultSetToString(ResultSet rs) throws SQLException {
        final StringBuilder sb = new StringBuilder();
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();

        // Print column headers
        for (int i = 1; i <= columnCount; i++) {
            sb.append(metaData.getColumnName(i));
            if (i < columnCount) {
                sb.append("\t");
            }
        }
        sb.append("\n");

        // Print rows
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
