package org.flywaydb.community.database.tibero;

import org.junit.jupiter.api.AfterEach;
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

    @AfterEach
    void clear() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(TIBERO_URL, USER, PASSWORD)) {

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE TIBERO.\"flyway_schema_history\"");
                statement.execute("DROP TABLE TEST");
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
            () -> assertThat(validateResult.invalidMigrations.get(0).description).isEqualTo("create test")
        );
    }
}
