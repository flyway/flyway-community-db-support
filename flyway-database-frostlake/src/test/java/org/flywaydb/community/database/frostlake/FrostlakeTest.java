/*-
 * ========================LICENSE_START=================================
 * flyway-database-frostlake
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
package org.flywaydb.community.database.frostlake;

import org.flywaydb.core.Flyway;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Frostlake is embeddable, so these tests run against an in-process engine
 * ({@code jdbc:frostlake:direct:<name>}) — no container required. Each test uses its own engine
 * name; engines are shared per name for the lifetime of the JVM.
 */
public class FrostlakeTest {
    private static final String LOCATION = "classpath:frostlake_migration";

    private Flyway flyway(String engineName) {
        return Flyway.configure()
                .dataSource("jdbc:frostlake:direct:" + engineName, null, null)
                .locations(LOCATION)
                .load();
    }

    @Test
    public void migrationsApplyOnceAndSkipOnRerun() {
        Flyway flyway = flyway("flyway_test_rerun");
        assertEquals(3, flyway.migrate().migrationsExecuted);
        assertEquals(0, flyway.migrate().migrationsExecuted);
    }

    @Test
    public void migratedObjectsAreQueryable() throws SQLException {
        flyway("flyway_test_objects").migrate();
        try (Connection connection = DriverManager.getConnection("jdbc:frostlake:direct:flyway_test_objects");
             Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM customers")) {
                assertTrue(rs.next());
                // Two seeded rows plus the one the V3 scripting block inserts.
                assertEquals(3, rs.getLong(1));
            }
            try (ResultSet rs = statement.executeQuery("CALL count_customers()")) {
                assertTrue(rs.next());
                assertEquals(3, ((Number) rs.getObject(1)).longValue());
            }
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM customer_names")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getLong(1));
            }
        }
    }

    @Test
    public void schemaHistoryRecordsEveryMigration() throws SQLException {
        flyway("flyway_test_history").migrate();
        try (Connection connection = DriverManager.getConnection("jdbc:frostlake:direct:flyway_test_history");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT \"version\", \"success\" FROM \"flyway_schema_history\" ORDER BY \"installed_rank\"")) {
            for (int version = 1; version <= 3; version++) {
                assertTrue(rs.next());
                assertEquals(String.valueOf(version), rs.getString(1));
                assertEquals(Boolean.TRUE, rs.getObject(2));
            }
            assertFalse(rs.next());
        }
    }
}
