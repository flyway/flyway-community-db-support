package org.flywaydb.community.database.tibero;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.flywaydb.community.database.tibero.FlywayForTibero.*;
import static org.junit.jupiter.api.Assertions.*;

class TiberoFlywayValidateTest {

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
    @DisplayName("validate test")
    void validateTest() {

        boolean success = createFlyway("classpath:db/migration").migrate().success;

        var flyway = createFlyway("classpath:db/migration-with-failed");

        var validateResult = flyway.validateWithResult();

        assertAll(
            () -> assertThat(success).isTrue(),
            () -> assertThat(validateResult.validationSuccessful).isFalse(),
            () -> assertThat(validateResult.errorDetails).isNotNull(),
            () -> assertThat(validateResult.invalidMigrations.get(0).description).isEqualTo("create failed test")
        );
    }
}
