package org.flywaydb.community.database.tibero;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TiberoBaselineTest {
    @AfterEach
    void clear() throws SQLException {
        try (Connection connection = DriverManager
            .getConnection("jdbc:tibero:thin:@localhost:8629:tibero", "tibero", "tibero")) {

            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE flyway_users");
                statement.execute("DROP TABLE TIBERO.\"flyway_schema_history\"");
            }
        }
    }


    @Test
    void baselineTest() throws SQLException {

        // create default table
        try (Connection connection = DriverManager
            .getConnection("jdbc:tibero:thin:@localhost:8629:tibero", "tibero", "tibero")) {

            try (Statement statement = connection.createStatement()) {

                statement.execute(
                    "create table flyway_users ( "
                        + "    id int primary key , "
                        + "    name varchar2(255) "
                        + ");"
                );
            }
        }

        Flyway flyway = Flyway.configure()
            .locations("classpath:db/migration")
            .dataSource("jdbc:tibero:thin:@localhost:8629:tibero", "tibero", "tibero")
            .baselineVersion("1")
            .load();

        flyway.baseline();

        List<String> baselineTables = new ArrayList<>();

        // check that the baseline was successful
        try (Connection connection = DriverManager
            .getConnection("jdbc:tibero:thin:@localhost:8629:tibero", "tibero", "tibero")) {

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(
                    "SELECT table_name " +
                        "FROM all_tables " +
                        "WHERE owner = 'TIBERO'"
                );

                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    baselineTables.add(tableName);
                }
            }
        }

        // check flyway schema history
        FlywaySchemaHistory history = null;
        try (Connection connection = DriverManager
            .getConnection("jdbc:tibero:thin:@localhost:8629:tibero", "tibero", "tibero")) {

            try (Statement statement = connection.createStatement()) {
                ResultSet rs = statement.executeQuery(
                    "SELECT * " +
                        "FROM TIBERO.\"flyway_schema_history\""
                );

                while (rs.next()) {
                    int installedRank = rs.getInt("installed_rank");
                    int version = rs.getInt("version");
                    String description = rs.getString("description");
                    String type = rs.getString("type");
                    String script = rs.getString("script");
                    long checkSum = rs.getLong("checksum");
                    boolean success = rs.getBoolean("success");

                    history = new FlywaySchemaHistory(installedRank, version,
                        description, type, script, checkSum, success);
                }
            }
        }

        FlywaySchemaHistory finalHistory = history;

        assertAll(
            () -> assertThat(finalHistory).isNotNull(),
            () -> assertThat(finalHistory.getInstalledRank()).isEqualTo(1),
            () -> assertThat(finalHistory.getVersion()).isEqualTo(1),
            () -> assertThat(finalHistory.getDescription()).isEqualTo("<< Flyway Baseline >>"),
            () -> assertThat(finalHistory.getType()).isEqualTo("BASELINE"),
            () -> assertThat(finalHistory.getScript()).isEqualTo("<< Flyway Baseline >>"),
            () -> assertThat(finalHistory.isSuccess()).isTrue(),
            () -> assertThat(baselineTables).contains("flyway_schema_history")
        );
    }
}