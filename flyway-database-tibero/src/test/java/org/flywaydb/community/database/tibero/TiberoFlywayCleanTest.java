package org.flywaydb.community.database.tibero;

import static org.assertj.core.api.Assertions.assertThat;
import static org.flywaydb.community.database.tibero.FlywayForTibero.TIBERO_URL;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.assertj.core.api.SoftAssertions;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.CleanResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TiberoFlywayCleanTest {

    private static final String TEST_USER = "HANI";
    private static final String TEST_PASSWORD = "PASSWORD";

    private static final String[] OBJECTS_INFO_TABLES = new String[]{
        "ALL_TABLES", "ALL_SYNONYMS", "ALL_VIEWS", "ALL_SCHEDULER_CHAINS",
        "ALL_SCHEDULER_PROGRAMS", "ALL_SCHEDULER_SCHEDULES"};

    @BeforeEach
    void setUp() throws SQLException {
        try (Connection connection = DriverManager
            .getConnection(TIBERO_URL, "sys", "tibero")) {
            try (Statement statement = connection.createStatement()) {

                // 1. check if test schema exists
                ResultSet resultSet = statement.executeQuery(
                    "SELECT username FROM dba_users WHERE username = '" + TEST_USER + "'");

                // 1-1. if exists, drop test schema
                if (resultSet.isBeforeFirst()) {
                    statement.execute("DROP USER " + TEST_USER + " CASCADE");
                }

                // 2. create test schema to test clean
                statement.execute(
                    "CREATE USER " + TEST_USER + " IDENTIFIED BY " + TEST_PASSWORD + " DEFAULT TABLESPACE TIBERO");
                statement.execute("GRANT RESOURCE, CONNECT, DBA TO " + TEST_USER);
            }
        }
    }

    @Test
    @DisplayName("clean test")
    void cleanTest() throws SQLException {
        SoftAssertions softAssertions = new SoftAssertions();

        Flyway flyway = Flyway.configure()
            .locations("classpath:db/migration-clean-test")
            .cleanDisabled(false)
            .dataSource(TIBERO_URL, TEST_USER, TEST_PASSWORD)
            .load();

        boolean success = flyway.migrate().success;
        CleanResult cleanResult = flyway.clean();

        // 1. Verify created objects were properly dropped
        try (Connection conn = DriverManager.getConnection(TIBERO_URL, TEST_USER, TEST_PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet objectCnt = stmt.executeQuery(getCountObjectsQuery(TEST_USER))) {

            objectCnt.next();
            softAssertions.assertThat(objectCnt.getInt(1)).isEqualTo(0);
        }

        assertAll(
            () -> assertThat(success).isTrue(),
            () -> assertThat(cleanResult.schemasCleaned).contains(TEST_USER)
        );
    }

    private String getCountObjectsQuery(String schema) {
        StringBuilder query = new StringBuilder();

        query.append("SELECT COUNT(*) FROM ( ");
        query.append("SELECT SEQUENCE_OWNER FROM ALL_SEQUENCES WHERE SEQUENCE_OWNER = '" + schema + "'");

        Arrays.stream(OBJECTS_INFO_TABLES)
            .forEach(table -> query.append(
                String.format(" UNION ALL SELECT OWNER FROM %s WHERE OWNER = '%s'", table, schema)));

        query.append(");");

        return query.toString();
    }
}
