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
import java.util.Arrays;
import org.assertj.core.api.SoftAssertions;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.CleanResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TiberoFlywayCleanTest {

    private static final String[] OBJECTS_INFO_TABLES = new String[]{
        "ALL_TABLES", "ALL_SYNONYMS", "ALL_VIEWS", "ALL_SCHEDULER_CHAINS",
        "ALL_SCHEDULER_PROGRAMS", "ALL_SCHEDULER_SCHEDULES"};

    @BeforeEach
    @AfterEach
    void cleanup() throws SQLException {
        createFlyway("classpath:db/migration-clean-test").clean();
    }

//    @BeforeEach // TODO : 지워줄 방법이 없다..
//    void clear() throws SQLException {
//        try (Connection connection = DriverManager
//            .getConnection(TIBERO_URL, USER, PASSWORD)) {
//
//            try (Statement statement = connection.createStatement()) {
//                // PSM compile error 혹은 없으면 예외를 던져버림..
//                statement.execute("CALL DBMS_SCHEDULER.CREATE_PROGRAM('MY_PROGRAM')");
//
//            }
//        }
//    }

    @Test
    @DisplayName("clean test")
    void cleanTest() throws SQLException {
        SoftAssertions softAssertions = new SoftAssertions();

        Flyway flyway = createFlyway("classpath:db/migration-clean-test");
        boolean success = flyway.migrate().success;
        CleanResult cleanResult = flyway.clean();

        // 1. Verify created objects were properly dropped
        try (Connection conn = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
            Statement stmt = conn.createStatement();
            ResultSet objectCnt = stmt.executeQuery(getCountObjectsQuery(SCHEMA))) {

            objectCnt.next();
            softAssertions.assertThat(objectCnt.getInt(1)).isEqualTo(0);
        }

        assertAll(
            () -> assertThat(success).isTrue(),
            () -> assertThat(cleanResult.schemasCleaned).contains(SCHEMA)

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
