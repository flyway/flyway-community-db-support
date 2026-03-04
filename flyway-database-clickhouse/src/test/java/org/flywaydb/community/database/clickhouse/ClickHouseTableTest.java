/*-
 * ========================LICENSE_START=================================
 * flyway-database-clickhouse
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
package org.flywaydb.community.database.clickhouse;


import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Testcontainers
class ClickHouseTableTest {

    @Container
    private static final ClickHouseContainer clickhouse = new ClickHouseContainer("clickhouse/clickhouse-server:latest").withUrlParam("compress", "0");


    private static final List<Object[]> EXPECTED_MIGRATION_HISTORY = Arrays.asList(
            new Object[]{"V1__create_test_flyway_table.sql", "create test flyway table", Boolean.TRUE},
            new Object[]{"V2__insert_3_rows.sql", "insert 3 rows", Boolean.TRUE},
            new Object[]{"V3__insert_count_row.sql", "insert count row", Boolean.TRUE}
    );
    private static final List<Object[]> EXPECTED_MIGRATION_ROWS = Arrays.asList(
            new Object[]{0, "payload"},
            new Object[]{1, "payload"},
            new Object[]{2, "payload"},
            new Object[]{3, "count"}
    );

    /**
     * Verifies that running migrate a second time when all migrations are already applied is a
     * no-op. Each migration script should still appear exactly once in the schema history and the
     * table data should be unchanged. V3 inserts count(*) as a key, so a double-apply would
     * produce a different row count and cause this assertion to fail.
     */
    @Test
    public void testMigrateIsIdempotent() throws Exception {
        Flyway flyway = buildFlyway();

        flyway.migrate();
        flyway.migrate(); // second run: all migrations already applied, should be a no-op

        assertMigrationsRanSuccessfully();
    }

    /**
     * Run the migration with specified concurrency of flyway executions. Without any locking they
     * will run concurrently and V1__create_test_flyway_table will fail as the table is already
     * created.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testConcurrency(int concurrency) throws Exception {

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {

            List<Future<?>> migrations = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                migrations.add(executor.submit(() -> {
                    try {
                        Flyway flyway = buildFlyway();

                        // wait and start all the migrations at the same time
                        assertTrue(startLatch.await(1, TimeUnit.MINUTES));
                        flyway.migrate();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown();

            for (Future<?> migration : migrations) {
                migration.get(10, TimeUnit.MINUTES);
            }

            assertMigrationsRanSuccessfully();

        } finally {
            startLatch.countDown();
            executor.shutdown();
        }
    }

    private Flyway buildFlyway() {
        return Flyway.configure()
                .dataSource(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword())
                .locations("classpath:locking_migration")
                .baselineVersion("0")
                .table("flyway_schema_history")
                .load();
    }

    private void assertMigrationsRanSuccessfully() throws Exception {
        // each script is run once and is successful

        // script, description, success
        List<Object[]> flywayHistory = null;
        int count = 0;

        while (flywayHistory == null && count++ < 100) {

            try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword());
                 Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("SELECT * from flyway_schema_history where script != '<< Flyway Baseline >>'")) {
                List<Object[]> actualHistory = new ArrayList<>();
                while (resultSet.next()) {
                    String description = resultSet.getString("description");
                    if ("flyway-lock".equals(description)) {
                        System.out.println(
                                "lock row found in history for " + resultSet.getString("version") + ", retrying..."
                        );
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        continue;
                    }
                    String script = resultSet.getString("script");
                    boolean success = resultSet.getBoolean("success");

                    actualHistory.add(new Object[]{script, description, success});
                }

                flywayHistory = actualHistory;
            }
        }

        assertRowsEquals(EXPECTED_MIGRATION_HISTORY, flywayHistory);

        // results match each script only running once
        try (Connection connection = DriverManager.getConnection(clickhouse.getJdbcUrl(), clickhouse.getUsername(), clickhouse.getPassword());
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT key, data from test_flyway_table")) {

            // Key, Data
            List<Object[]> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(new Object[]{resultSet.getInt("key"), resultSet.getString("data")});
            }

            assertRowsEquals(EXPECTED_MIGRATION_ROWS, results);
        }
    }

    private void assertRowsEquals(List<Object[]> expected, List<Object[]> actual) {
        assertEquals(expected.stream().map(Arrays::toString).toList(), actual.stream().map(Arrays::toString).sorted().toList());
    }
}
