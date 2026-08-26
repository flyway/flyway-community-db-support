/*-
 * ========================LICENSE_START=================================
 * flyway-database-dsql
 * ========================================================================
 * Copyright (C) 2010 - 2026 Red Gate Software Ltd
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
package org.flywaydb.community.database.dsql;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.migration.Context;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.output.MigrateResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end live-cluster test for the Aurora DSQL OCC retry.
 *
 * <p>Aurora DSQL is a managed AWS service with no local, embedded, or Testcontainers equivalent,
 * so this test cannot run in CI. It is gated on the {@code DSQL_CLUSTER_ENDPOINT} environment
 * variable and is silently skipped whenever that variable is absent (i.e. always, in CI). The
 * deterministic {@link DSQLExecutionTemplateTest} carries the retry regression coverage that does
 * run in CI; this test is the on-demand proof against a real cluster.
 *
 * <p>It drives the full Flyway stack — {@link Flyway#migrate()} against a live cluster through the
 * {@code jdbc:aws-dsql:} connector — and induces a <em>real</em> commit-time OCC conflict from
 * inside a Java migration: on its first attempt the migration writes row 1 on Flyway's own
 * connection, then a second connection commits a conflicting write to the same row before the
 * migration returns. Flyway's {@code commit()} therefore loses the OCC race and surfaces a genuine
 * {@code OC000}/{@code OC001}/{@code 40001} SQLSTATE, which the module's {@link DSQLExecutionTemplate}
 * (installed via {@link DSQLDatabaseType}) retries. The retry re-runs the migration with no
 * conflict and commits, so {@code migrate()} succeeds and the retry's value is what persists.
 *
 * <p>Run locally with AWS credentials on the default provider chain and:
 * <pre>{@code
 * export DSQL_CLUSTER_ENDPOINT=<cluster-id>.dsql.<region>.on.aws
 * ./mvnw -pl flyway-database-dsql test -Dtest=DSQLIntegrationTest
 * }</pre>
 * All work stays in an isolated {@code flyway_dsql_test} schema; it never touches {@code public}.
 */
@EnabledIfEnvironmentVariable(named = "DSQL_CLUSTER_ENDPOINT", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DSQLIntegrationTest {

    private static final String SCHEMA = "flyway_dsql_test";
    private static final String TABLE = SCHEMA + ".occ_e2e";
    private static final String HISTORY = SCHEMA + ".flyway_schema_history";

    private String jdbcUrl() {
        return "jdbc:aws-dsql:postgresql://" + System.getenv("DSQL_CLUSTER_ENDPOINT") + "/postgres";
    }

    private String user() {
        return System.getenv().getOrDefault("DSQL_CLUSTER_USER", "admin");
    }

    private Connection open() throws Exception {
        Properties props = new Properties();
        // The connector generates the IAM token from the default AWS credential chain and parses
        // the region from the endpoint hostname; no password is supplied.
        props.setProperty("user", user());
        return DriverManager.getConnection(jdbcUrl(), props);
    }

    // Schema and table DDL runs once per class, not per test: DSQL's catalog is distributed and
    // reactively discovered, so repeating CREATE/DROP TABLE races other work and itself raises
    // catalog OC001 conflicts. The schema is pre-created because Flyway bootstraps its history
    // table on a separate connection that cannot yet see a just-created schema.
    @BeforeAll
    void createSchemaAndTable() throws Exception {
        try (Connection c = open(); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            s.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            s.execute("DROP TABLE IF EXISTS " + HISTORY);
            s.execute("DROP TABLE IF EXISTS " + TABLE);
            s.execute("CREATE TABLE " + TABLE + " (id int PRIMARY KEY, val int)");
        }
    }

    @AfterAll
    void dropTable() throws Exception {
        try (Connection c = open(); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            s.execute("DROP TABLE IF EXISTS " + TABLE);
        }
    }

    @BeforeEach
    void resetState() throws Exception {
        // DML only: repeating DDL here races DSQL's distributed catalog and raises OCC conflicts,
        // so all table/history DDL lives in the once-per-class setup above.
        try (Connection c = open(); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            s.execute("DELETE FROM " + TABLE);
            s.execute("INSERT INTO " + TABLE + " (id, val) VALUES (1, 0)");
            // Each test applies its own version-1 migration with a distinct checksum against the
            // shared schema, so clear any prior test's history rows; otherwise Flyway's validation
            // fails the next migrate with a checksum mismatch. DML only — dropping the history
            // table would repeat DDL on the same object and race the catalog (see @BeforeAll).
            if (historyExists(s)) {
                s.execute("DELETE FROM " + HISTORY);
            }
        }
    }

    private boolean historyExists(Statement s) throws Exception {
        try (ResultSet rs = s.executeQuery(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = '" + SCHEMA
                        + "' AND table_name = 'flyway_schema_history'")) {
            return rs.next();
        }
    }

    @Test
    void migrateRetriesCommitTimeOccConflictAndSucceeds() {
        AtomicInteger attempts = new AtomicInteger();

        FluentConfiguration config = Flyway.configure()
                .dataSource(jdbcUrl(), user(), null)
                .schemas(SCHEMA)
                // The schema already holds the fixture table (occ_e2e), so baseline the empty
                // history rather than requiring an empty schema. Baseline at v0 so it stays
                // distinct from the v1 migration under test.
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .javaMigrations(new ConflictOnFirstAttempt(attempts));
        // OCC retry is off by default; opt in so the commit-time conflict is retried.
        config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class)
                .setOccMaxRetries(3);
        Flyway flyway = config.load();

        MigrateResult result = flyway.migrate();

        assertThat(result.success).isTrue();
        assertThat(result.migrationsExecuted).isEqualTo(1);
        // The migration ran twice: the first commit lost the OCC race and the module retried it.
        assertThat(attempts.get()).isEqualTo(2);
        // The retry's write (102) is what persisted, overwriting the conflicting 999.
        assertThat(committedVal()).isEqualTo(102);
        assertThat(appliedVersions()).contains("1");
        // The retry's history insert commits on the auto-commit main connection before the failed
        // commit, orphaning the first attempt's row. DSQLConnection reconciles it, so exactly one
        // successful history row must remain for the version (not two).
        assertThat(successfulRowCount("1")).isEqualTo(1);
    }

    @Test
    void migrateWaitsForAsyncIndexBuild() throws Exception {
        // Populate a table large enough that CREATE INDEX ASYNC lags the statement, then run a
        // SQL migration that creates the index ASYNC. If the wait fires, the index is fully built
        // (not 'building') the moment migrate() returns. This check is timing-dependent — a build
        // fast enough to finish before the status read would pass even without the wait; the
        // deterministic proof that the wait blocks and inspects the result is
        // migrateFailsWhenAsyncIndexBuildFails, which can only fail the migration if it does.
        String idxTable = SCHEMA + ".async_idx_e2e";
        int rows = 60000;
        int batch = 2500;   // DSQL caps mutations at 3,000 rows per transaction; stay under it.
        try (Connection c = open(); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            s.execute("DROP TABLE IF EXISTS " + idxTable);
            s.execute("CREATE TABLE " + idxTable + " (id int PRIMARY KEY, val int)");
            for (int start = 1; start <= rows; start += batch) {
                int end = Math.min(start + batch - 1, rows);
                s.execute("INSERT INTO " + idxTable
                        + " SELECT g, g FROM generate_series(" + start + ", " + end + ") g");
            }
        }

        try {
            java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("dsql-async-idx");
            // Leading comment on the statement exercises comment-stripping: the wait must still
            // recognize CREATE INDEX ASYNC and block on the build (Flyway keeps the comment in
            // Result.sql()).
            java.nio.file.Files.writeString(dir.resolve("V1__create_async_index.sql"),
                    "-- build the value index\n"
                            + "CREATE INDEX ASYNC async_idx_e2e_val ON " + idxTable + " (val);\n");

            FluentConfiguration config = Flyway.configure()
                    .dataSource(jdbcUrl(), user(), null)
                    .schemas(SCHEMA)
                    .baselineOnMigrate(true)
                    .baselineVersion("0")
                    .locations("filesystem:" + dir.toAbsolutePath());
            // The wait is opt-in; enable it so the migration blocks until the build completes.
            config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class)
                    .setAwaitAsyncIndexes(true);
            Flyway flyway = config.load();

            MigrateResult result = flyway.migrate();
            assertThat(result.success).isTrue();
            assertThat(result.migrationsExecuted).isEqualTo(1);

            // The index must be present and no longer building the instant migrate() returned.
            // object_name is schema-qualified (e.g. flyway_dsql_test.async_idx_e2e_val), so match
            // the qualified name built from SCHEMA rather than the bare index name.
            try (Connection c = open(); Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT status FROM sys.jobs WHERE object_name = '"
                                 + SCHEMA + ".async_idx_e2e_val'")) {
                // sys.jobs purges completed rows after ~30 min; either it is gone (completed+purged)
                // or, if still listed, it must be 'completed' — never 'submitted'/'building'.
                if (rs.next()) {
                    assertThat(rs.getString(1)).isEqualTo("completed");
                }
            }
        } finally {
            try (Connection c = open(); Statement s = c.createStatement()) {
                c.setAutoCommit(true);
                s.execute("DROP TABLE IF EXISTS " + idxTable);
            }
        }
    }

    @Test
    void migrateFailsWhenAsyncIndexBuildFails() throws Exception {
        // Deterministic failure: a UNIQUE index over duplicate values cannot build, so
        // sys.wait_for_job returns false and the wait must fail the migration. Unlike the success
        // test this does not depend on build timing — the build is guaranteed to fail.
        String idxTable = SCHEMA + ".async_idx_fail";
        try (Connection c = open(); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            s.execute("DROP TABLE IF EXISTS " + idxTable);
            s.execute("CREATE TABLE " + idxTable + " (id int PRIMARY KEY, val int)");
            // Two rows sharing val = 1 make a UNIQUE index on val impossible.
            s.execute("INSERT INTO " + idxTable + " (id, val) VALUES (1, 1), (2, 1)");
        }

        try {
            java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("dsql-async-idx-fail");
            java.nio.file.Files.writeString(dir.resolve("V1__create_unique_async_index.sql"),
                    "CREATE UNIQUE INDEX ASYNC async_idx_fail_val ON " + idxTable + " (val);\n");

            FluentConfiguration config = Flyway.configure()
                    .dataSource(jdbcUrl(), user(), null)
                    .schemas(SCHEMA)
                    .baselineOnMigrate(true)
                    .baselineVersion("0")
                    .locations("filesystem:" + dir.toAbsolutePath());
            config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class)
                    .setAwaitAsyncIndexes(true);
            Flyway flyway = config.load();

            assertThatThrownBy(flyway::migrate).hasMessageContaining("async index build");
        } finally {
            try (Connection c = open(); Statement s = c.createStatement()) {
                c.setAutoCommit(true);
                s.execute("DROP TABLE IF EXISTS " + idxTable);
            }
        }
    }

    /**
     * A transactional Java migration that induces a commit-time OCC conflict on its first attempt.
     * It writes row 1 on Flyway's connection, then — only on attempt 1 — commits a conflicting
     * write to the same row from a second connection, so Flyway's commit loses the OCC race.
     */
    private class ConflictOnFirstAttempt implements JavaMigration {
        private final AtomicInteger attempts;

        ConflictOnFirstAttempt(AtomicInteger attempts) {
            this.attempts = attempts;
        }

        @Override
        public MigrationVersion getVersion() {
            return MigrationVersion.fromVersion("1");
        }

        @Override
        public String getDescription() {
            return "occ conflict retry";
        }

        @Override
        public Integer getChecksum() {
            return 1;
        }

        @Override
        public boolean canExecuteInTransaction() {
            return true;
        }

        @Override
        public void migrate(Context context) throws Exception {
            int attempt = attempts.incrementAndGet();
            try (Statement s = context.getConnection().createStatement()) {
                s.executeUpdate("UPDATE " + TABLE + " SET val = " + (100 + attempt) + " WHERE id = 1");
            }
            if (attempt == 1) {
                try (Connection connB = open(); Statement s = connB.createStatement()) {
                    connB.setAutoCommit(false);
                    s.executeUpdate("UPDATE " + TABLE + " SET val = 999 WHERE id = 1");
                    connB.commit();
                }
            }
        }
    }

    private int committedVal() {
        try (Connection c = open(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM " + TABLE + " WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            return rs.getInt(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int successfulRowCount(String version) {
        try (Connection c = open(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT COUNT(*) FROM " + HISTORY + " WHERE success = true AND version = '" + version + "'")) {
            assertThat(rs.next()).isTrue();
            return rs.getInt(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> appliedVersions() {
        List<String> versions = new ArrayList<>();
        try (Connection c = open(); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT version FROM " + HISTORY + " WHERE success = true ORDER BY installed_rank")) {
            while (rs.next()) {
                versions.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return versions;
    }
}
