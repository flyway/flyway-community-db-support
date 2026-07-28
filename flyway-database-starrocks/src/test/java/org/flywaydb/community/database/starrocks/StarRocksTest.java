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

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;
import java.time.Duration;

import static org.junit.Assert.*;

public class StarRocksTest {
    private static final DockerImageName STARROCKS_IMAGE = DockerImageName.parse("starrocks/allin1-ubuntu:latest");
    private static final int FE_MYSQL_PORT = 9030;
    private static final int FE_HTTP_PORT = 8030;
    private static final String LOCATION = "starrocks_migration";
    private static final String USER = "root";
    private static final String PWD = "";
    private static final String DATABASE = "flyway_test";
    private static final long BACKEND_WAIT_TIMEOUT_MS = 120000;
    private static final long BACKEND_POLL_INTERVAL_MS = 2000;

    private static GenericContainer<?> starrocks;

    @Before
    public void setUp() throws SQLException {
        // Skip tests if Docker is not available
        boolean dockerAvailable = false;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception e) {
            // Docker not available
        }
        Assume.assumeTrue("Docker is not available, skipping test", dockerAvailable);

        // Start container only if not already started
        if (starrocks == null || !starrocks.isRunning()) {
            starrocks = new GenericContainer<>(STARROCKS_IMAGE)
                    .withExposedPorts(FE_MYSQL_PORT, FE_HTTP_PORT)
                    .waitingFor(Wait.forHttp("/api/health")
                            .forPort(FE_HTTP_PORT)
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(5)))
                    .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
            starrocks.start();
            
            // Wait for backend node to be registered with the frontend
            waitForBackendReady();
        }
    }

    /**
     * Waits for the StarRocks backend node to be registered and alive.
     * The all-in-one container needs time for the BE to register with the FE after startup.
     */
    private void waitForBackendReady() throws SQLException {
        String jdbcUrl = jdbcUrl();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + BACKEND_WAIT_TIMEOUT_MS;
        
        while (System.currentTimeMillis() < endTime) {
            try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PWD);
                 Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery("SHOW BACKENDS")) {
                
                while (rs.next()) {
                    boolean alive = rs.getBoolean("Alive");
                    if (alive) {
                        System.out.println("StarRocks backend is ready and alive");
                        return;
                    }
                }
            } catch (SQLException e) {
                // Connection might fail during startup, continue waiting
                System.out.println("Waiting for StarRocks backend to be ready: " + e.getMessage());
            }
            
            try {
                Thread.sleep(BACKEND_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for backend", e);
            }
        }
        
        throw new RuntimeException("Timeout waiting for StarRocks backend to be ready after " + 
                BACKEND_WAIT_TIMEOUT_MS + "ms");
    }

    @Test
    public void testMigration() throws SQLException {
        final String jdbcUrl = jdbcUrl();

        // Create the test database first
        createDatabase(jdbcUrl);

        final Flyway flyway = flyway(jdbcUrl + "/" + DATABASE);
        final MigrateResult migrateResult = flyway.migrate();

        assertEquals(0, migrateResult.getFailedMigrations().size());
        assertTrue("Expected migrations to be applied", migrateResult.migrationsExecuted > 0);

        // Verify the table was created
        assertTableExists(jdbcUrl + "/" + DATABASE, "users");
    }

    private String jdbcUrl() {
        final String host = starrocks.getHost();
        final int port = starrocks.getMappedPort(FE_MYSQL_PORT);
        return "jdbc:mysql://" + host + ":" + port;
    }

    private void createDatabase(String jdbcUrl) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PWD);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
        }
    }

    private static Flyway flyway(String jdbcUrl) {
        return Flyway
                .configure()
                .locations(LOCATION)
                .dataSource(jdbcUrl, USER, PWD)
                .load();
    }

    private static void assertTableExists(String jdbcUrl, String tableName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PWD);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SHOW TABLES LIKE '" + tableName + "'")) {
            assertTrue("Table " + tableName + " should exist", rs.next());
        }
    }
}
