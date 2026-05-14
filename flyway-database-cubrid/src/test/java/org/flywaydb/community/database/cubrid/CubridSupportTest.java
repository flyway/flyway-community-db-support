package org.flywaydb.community.database.cubrid;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class CubridSupportTest {
    private static final int BROKER_PORT = 33000;
    private static final String DATABASE_NAME = "cubrid";
    private static final String USER_SCHEMA = "public";
    private static final String INITIAL_MIGRATION_LOCATION = "cubrid_initial_migration";
    private static final String NEXT_MIGRATION_LOCATION = "cubrid_next_migration";

    @Container
    static final GenericContainer<?> cubrid = new GenericContainer<>(DockerImageName.parse("cubrid/cubrid:11.4"))
        .withEnv("CUBRID_DB", DATABASE_NAME)
        .withExposedPorts(BROKER_PORT)
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));

    @BeforeAll
    static void waitUntilJdbcIsReady() throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        Exception lastFailure = null;

        while (System.nanoTime() < deadline) {
            try (Connection connection = connection();
                 Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
                return;
            } catch (Exception e) {
                lastFailure = e;
                Thread.sleep(1_000);
            }
        }

        if (lastFailure != null) {
            throw lastFailure;
        }
        throw new IllegalStateException("CUBRID JDBC endpoint was not ready");
    }

    @BeforeEach
    void resetDatabase() throws SQLException {
        try (Connection connection = connection();
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS app_user");
            statement.execute("DROP TABLE IF EXISTS flyway_schema_history");
        }
    }

    @Test
    void migrates() throws SQLException {
        Flyway flyway = flyway(INITIAL_MIGRATION_LOCATION);

        assertThat(tableNames()).doesNotContain("app_user", "flyway_schema_history");

        flyway.migrate();

        assertThat(tableNames()).contains("app_user", "flyway_schema_history");
        assertThat(countAppUserRows()).isEqualTo(2);
        assertThat(historyDescriptions()).containsExactly("create app user", "insert app users");
    }

    @Test
    void doesNotApplyMigrationSeveralTimes() throws SQLException {
        Flyway flyway = flyway(INITIAL_MIGRATION_LOCATION);

        flyway.migrate();
        flyway.migrate();
        flyway.migrate();

        assertThat(countAppUserRows()).isEqualTo(2);
        assertThat(historyDescriptions()).containsExactly("create app user", "insert app users");
    }

    @Test
    void appliesOnlyNewMigrationsWhenSomeWereAlreadyApplied() throws SQLException {
        flyway(INITIAL_MIGRATION_LOCATION).migrate();

        assertThat(countAppUserRows()).isEqualTo(2);
        assertThat(historyDescriptions()).containsExactly("create app user", "insert app users");

        flyway(INITIAL_MIGRATION_LOCATION, NEXT_MIGRATION_LOCATION).migrate();

        assertThat(countAppUserRows()).isEqualTo(3);
        assertThat(historyDescriptions()).containsExactly("create app user", "insert app users", "insert more app users");
    }

    @Test
    void validatesAppliedMigrations() {
        Flyway flyway = flyway(INITIAL_MIGRATION_LOCATION);

        flyway.migrate();

        flyway.validate();
    }

    @Test
    void setsBaseline() throws SQLException {
        Flyway flyway = Flyway.configure()
            .communityDBSupportEnabled(true)
            .dataSource(jdbcUrl(), USER_SCHEMA, "")
            .defaultSchema(USER_SCHEMA)
            .locations(INITIAL_MIGRATION_LOCATION)
            .baselineVersion("123")
            .load();

        flyway.baseline();

        assertThat(tableNames()).contains("flyway_schema_history");
        assertThat(historyDescriptions()).containsExactly("<< Flyway Baseline >>");
    }

    @Test
    void cleansUserTables() throws SQLException {
        Flyway flyway = Flyway.configure()
            .communityDBSupportEnabled(true)
            .dataSource(jdbcUrl(), USER_SCHEMA, "")
            .defaultSchema(USER_SCHEMA)
            .locations(INITIAL_MIGRATION_LOCATION)
            .cleanDisabled(false)
            .load();

        flyway.migrate();
        assertThat(tableNames()).contains("app_user", "flyway_schema_history");

        flyway.clean();

        assertThat(tableNames()).doesNotContain("app_user", "flyway_schema_history");
    }

    @Test
    void cleansTablesWithForeignKeys() throws SQLException {
        Flyway flyway = Flyway.configure()
            .communityDBSupportEnabled(true)
            .dataSource(jdbcUrl(), USER_SCHEMA, "")
            .defaultSchema(USER_SCHEMA)
            .cleanDisabled(false)
            .load();

        try {
            try (Connection connection = connection();
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE parent_table (id INT PRIMARY KEY)");
                statement.execute("CREATE TABLE child_table (id INT PRIMARY KEY, parent_id INT, "
                    + "CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES parent_table(id))");
            }

            assertThat(tableNames()).contains("parent_table", "child_table");

            flyway.clean();

            assertThat(tableNames()).doesNotContain("parent_table", "child_table");
        } finally {
            try (Connection connection = connection();
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS child_table");
                statement.execute("DROP TABLE IF EXISTS parent_table");
            }
        }
    }

    private static Flyway flyway(String... locations) {
        return Flyway.configure()
            .communityDBSupportEnabled(true)
            .dataSource(jdbcUrl(), USER_SCHEMA, "")
            .defaultSchema(USER_SCHEMA)
            .locations(locations)
            .load();
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), USER_SCHEMA, "");
    }

    private static String jdbcUrl() {
        return "jdbc:cubrid:" + cubrid.getHost() + ":" + cubrid.getMappedPort(BROKER_PORT) + ":" + DATABASE_NAME + ":::";
    }

    private static Set<String> tableNames() throws SQLException {
        return Set.copyOf(queryForStrings(
            "SELECT class_name FROM db_class WHERE class_type = 'CLASS' AND is_system_class = 'NO'"
        ));
    }

    private static List<String> historyDescriptions() throws SQLException {
        return queryForStrings(
            "SELECT description FROM flyway_schema_history ORDER BY installed_rank"
        );
    }

    private static int countAppUserRows() throws SQLException {
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM app_user")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private static List<String> queryForStrings(String sql) throws SQLException {
        try (Connection connection = connection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            List<String> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(resultSet.getString(1));
            }
            return results;
        }
    }

}
