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
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.*;

import static org.junit.Assert.*;

public class QuestDBTest {
    private static final DockerImageName QUESTDB_IMAGE = DockerImageName.parse("questdb/questdb:nightly");
    private static final int HTTP_PORT = 9000;
    private static final int PG_PORT = 8812;

    @ClassRule
    public static GenericContainer<?> questdb = new GenericContainer<>(QUESTDB_IMAGE)
            .withExposedPorts(HTTP_PORT, PG_PORT)
            .withLogConsumer(outputFrame -> System.out.println(outputFrame.getUtf8String()));
    
    @Test
    public void testQuestDBContainer() throws SQLException {
        final String host = questdb.getHost();
        final int port = questdb.getMappedPort(PG_PORT);

        try (Connection connection = DriverManager.getConnection(
                "jdbc:postgresql://" + host + ":" + port + "/qdb", "admin", "quest"
        )) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery("select 1")) {
                resultSet.next();
                assertEquals(1, resultSet.getInt(1));
            }
        }
    }
    
    @Test
    public void testQuestDBFlyway() {
        final String host = questdb.getHost();
        final int port = questdb.getMappedPort(PG_PORT);

        final Flyway flyway = Flyway.configure().dataSource(
                "jdbc:postgresql://" + host + ":" + port + "/qdb", "admin", "quest"
        ).load();

        flyway.migrate();
    }
}
