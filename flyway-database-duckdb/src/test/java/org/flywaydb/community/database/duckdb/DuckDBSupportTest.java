package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DuckDBSupportTest {

    private static final String NEXT_MIGRATION_LOCATION = "next_migration";
    private static final String TEST_DB_FILENAME = "target/test.db";
    private static final String TEST_DB_CONNECTION_URL = "jdbc:duckdb:" + TEST_DB_FILENAME;
    private static final String INITIAL_MIGRATION_LOCATION = "initial_migration";

    private final JdbcTemplate jdbcTemplate = jdbcTemplate();

    DuckDBSupportTest() throws SQLException {
    }

    @AfterEach
    void cleanup() throws SQLException {
        jdbcTemplate.getConnection().close();
        new File(TEST_DB_FILENAME).delete();
    }

    @Test
    void migrates() throws SQLException {
        // given
        final var flyway = Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(INITIAL_MIGRATION_LOCATION)
            .load();

        assertThat(getAllTablesNames("some_schema")).isEmpty();

        // when
        flyway.migrate();

        // then
        assertThat(getAllTablesNames("main")).isEqualTo(List.of("flyway_schema_history", "some_table"));
        assertThat(getFlywayHistoryMigrationDescriptions()).isEqualTo(List.of("first", "second"));
        assertThat(getAllViewsNames()).contains("some_view");
        assertThat(getAllMacrosNames()).contains("some_is_empty");
        assertThat(getAllSequencesNames()).isEqualTo(List.of("some_sequence"));
    }

    @Test
    void does_not_apply_a_migration_several_times() throws SQLException {
        // given
        final var flyway = Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(INITIAL_MIGRATION_LOCATION)
            .load();

        // when
        flyway.migrate();
        flyway.migrate();
        flyway.migrate();

        // then
        assertThat(countSomeTableRows()).isEqualTo(2);
        assertThat(getFlywayHistoryMigrationDescriptions()).isEqualTo(List.of("first", "second"));
    }

    @Test
    void applies_only_new_migrations_when_some_was_already_applied() throws SQLException {
        // given
        final var flyway = Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(INITIAL_MIGRATION_LOCATION)
            .load();

        flyway.migrate();
        assertThat(countSomeTableRows()).isEqualTo(2);
        assertThat(getFlywayHistoryMigrationDescriptions()).isEqualTo(List.of("first", "second"));

        // when
        Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(NEXT_MIGRATION_LOCATION)
            .load()
            .migrate();

        // then
        assertThat(countSomeTableRows()).isEqualTo(3);
        assertThat(getFlywayHistoryMigrationDescriptions()).isEqualTo(List.of("first", "second", "add more rows"));
    }

    @Test
    void applies_migrations_to_non_default_schema() throws SQLException {
        // given
        final var flyway = Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(INITIAL_MIGRATION_LOCATION)
            .defaultSchema("some_schema")
            .load();

        assertThat(getAllTablesNames("some_schema")).isEmpty();

        // when
        flyway.migrate();

        // then
        assertThat(getAllTablesNames("main")).isEmpty();
        assertThat(getAllTablesNames("some_schema")).isEqualTo(List.of("flyway_schema_history", "some_table"));
    }

    @Test
    void sets_baseline() throws SQLException {
        // given
        final var flyway = Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(INITIAL_MIGRATION_LOCATION)
            .baselineVersion("123")
            .load();

        assertThat(getAllTablesNames("main")).isEmpty();

        // when
        flyway.baseline();

        // then
        assertThat(getAllTablesNames("main")).isEqualTo(List.of("flyway_schema_history"));
        assertThat(getFlywayHistoryMigrationDescriptions()).isEqualTo(List.of("<< Flyway Baseline >>"));
    }

    @Test
    void cleans_schema_ignoring_system_objects() throws SQLException {
        // given
        final var flyway = Flyway.configure()
            .dataSource(TEST_DB_CONNECTION_URL, "", "")
            .locations(INITIAL_MIGRATION_LOCATION)
            .cleanDisabled(false)
            .load();

        final var systemViews = getAllViewsNames();
        final var systemMacros = getAllMacrosNames();

        flyway.migrate();
        assertThat(getAllTablesNames("main")).isEqualTo(List.of("flyway_schema_history", "some_table"));
        assertThat(getAllViewsNames().size()).isGreaterThan(systemViews.size());
        assertThat(getAllMacrosNames().size()).isGreaterThan(systemMacros.size());
        assertThat(getAllSequencesNames()).isEqualTo(List.of("some_sequence"));

        // when
        flyway.clean();

        // then
        assertThat(getAllTablesNames("main")).isEmpty();
        assertThat(getAllViewsNames()).isEqualTo(systemViews);
        assertThat(getAllMacrosNames()).isEqualTo(systemMacros);
        assertThat(getAllSequencesNames()).isEmpty();
    }

    private List<String> getAllTablesNames(String schema) throws SQLException {
        return jdbcTemplate.queryForStringList("SELECT table_name FROM duckdb_tables() WHERE schema_name = ?", schema);
    }

    private List<String> getAllViewsNames() throws SQLException {
        return jdbcTemplate.queryForStringList("SELECT view_name FROM duckdb_views()");
    }

    private List<String> getAllMacrosNames() throws SQLException {
        return jdbcTemplate.queryForStringList("SELECT function_name FROM duckdb_functions()");
    }

    private List<String> getAllSequencesNames() throws SQLException {
        return jdbcTemplate.queryForStringList("SELECT sequence_name FROM duckdb_sequences()");
    }

    private Integer countSomeTableRows() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT count(*) FROM main.some_table");
    }

    private List<String> getFlywayHistoryMigrationDescriptions() throws SQLException {
        return jdbcTemplate.queryForStringList("SELECT description FROM main.flyway_schema_history ORDER by installed_rank");
    }

    private JdbcTemplate jdbcTemplate() throws SQLException {
        return new JdbcTemplate(
            DriverManager.getConnection("jdbc:duckdb:" + TEST_DB_FILENAME, "", ""),
            new DuckDBDatabaseType()
        );
    }
}