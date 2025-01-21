package org.flywaydb.community.database.tibero;

import static org.flywaydb.community.database.tibero.FlywayForTibero.PASSWORD;
import static org.flywaydb.community.database.tibero.FlywayForTibero.SCHEMA;
import static org.flywaydb.community.database.tibero.FlywayForTibero.TIBERO_URL;
import static org.flywaydb.community.database.tibero.FlywayForTibero.USER;
import static org.flywaydb.community.database.tibero.FlywayForTibero.createFlyway;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


public class TiberoFlywayMigrationTest {

    private static final String[] EVOLUTION_SCHEMA_MIGRATION_DIRS = new String[]{
        "migration-step-1", "migration"};
    private static final String[] EVOLUTION_SCHEMA_MIGRATION_SCRIPT_NAMES = new String[]{
        "V1__create_tables.sql", "V2__insert_data.sql"};

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
    @DisplayName("migration simple test")
    void migrationTest() {
        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(createFlyway("classpath:db/migration").migrate().success).isTrue();
    }

    @Test
    @DisplayName("migration evolution test")
    void evolutionSchemaTest() throws SQLException {
        SoftAssertions softAssertions = new SoftAssertions();

        for (String migrationDir : EVOLUTION_SCHEMA_MIGRATION_DIRS) {
            softAssertions.assertThat(createFlyway("classpath:db/" + migrationDir).migrate().success).isTrue();
        }

        // 1. Verify that the flyway_schema_history table is properly logged after migration
        try (Connection conn = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet schema_history_cnt = stmt.executeQuery(
                "SELECT COUNT(*) FROM TIBERO.\"flyway_schema_history\" WHERE \"success\" = 1")) {

            schema_history_cnt.next();
            softAssertions.assertThat(schema_history_cnt.getInt(1)).isEqualTo(2);
        }

        // 2. Verify script names
        try (Connection conn = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet script_names = stmt.executeQuery(
                "SELECT \"script\" FROM TIBERO.\"flyway_schema_history\" WHERE \"success\" = 1")) {
            while (script_names.next()) {
                softAssertions.assertThat(script_names.getString(1))
                    .containsAnyOf(EVOLUTION_SCHEMA_MIGRATION_SCRIPT_NAMES);
            }
        }

        softAssertions.assertAll();
    }
}
