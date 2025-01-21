package org.flywaydb.community.database.tibero;

import static org.assertj.core.api.Assertions.assertThat;
import static org.flywaydb.community.database.tibero.FlywayForTibero.PASSWORD;
import static org.flywaydb.community.database.tibero.FlywayForTibero.SCHEMA;
import static org.flywaydb.community.database.tibero.FlywayForTibero.TIBERO_URL;
import static org.flywaydb.community.database.tibero.FlywayForTibero.USER;
import static org.flywaydb.community.database.tibero.FlywayForTibero.createFlyway;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.flywaydb.core.api.MigrationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TiberoFlywayInfoTest {

    @BeforeEach
    @AfterEach
    void cleanup() throws SQLException {
        try (Connection connection = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                "SELECT TABLE_NAME FROM USER_TABLES WHERE TABLESPACE_NAME = " + "'" + SCHEMA + "'")) {

            List<String> tables = new ArrayList<>();
            while (resultSet.next()) {
                tables.add(resultSet.getString("TABLE_NAME"));
            }

            for (String tableName : tables) {
                String dropTableQuery = "DROP TABLE \"" + tableName + "\" CASCADE CONSTRAINTS";
                statement.executeUpdate(dropTableQuery);
            }
        }
    }

    @Test
    @DisplayName("info test")
    void infoTest() {

        boolean success = createFlyway("classpath:db/migration-info-test").migrate().success;

        var flyway = createFlyway("classpath:db/migration-info-test");
        var info = flyway.info().all();

        int actualNumOfVersion = info.length;
        MigrationInfo history1 = info[0];
        MigrationInfo history2 = info[1];
        MigrationInfo history3 = info[2];

        assertAll(
            () -> assertThat(success).isTrue(),
            () -> assertThat(actualNumOfVersion).isEqualTo(3),
            () -> assertThat(Integer.parseInt(history1.getVersion().toString())).isEqualTo(1),
            () -> assertThat(history1.getScript()).isEqualTo("V1__create_my_tables.sql"),
            () -> assertThat(Integer.parseInt(history2.getVersion().toString())).isEqualTo(2),
            () -> assertThat(history2.getScript()).isEqualTo("V2__create_my_users.sql"),
            () -> assertThat(Integer.parseInt(history3.getVersion().toString())).isEqualTo(3),
            () -> assertThat(history3.getScript()).isEqualTo("V3__create_my_posts.sql")
        );
    }
}