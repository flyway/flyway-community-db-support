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

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.jdbc.WarningImpl;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class DSQLSqlScriptExecutorTest {

    // Records CALL statements prepared on the connection and the bound job id, and simulates the
    // sys.wait_for_job result: execute() reports a result set whose single boolean column returns
    // the configured outcome (true = build succeeded, false = build failed). Optionally makes
    // execute() raise a SQLException instead, to exercise the belt-and-suspenders catch.
    private static class RecordingConnection {
        final List<String> preparedCalls = new ArrayList<>();
        final List<String> boundJobIds = new ArrayList<>();
        // Starts in a transaction (autoCommit false), like the connection Flyway hands the executor.
        boolean autoCommit = false;
        // autoCommit state captured when prepareCall was invoked, so a test can assert the wait
        // ran outside a transaction block.
        Boolean autoCommitAtPrepareCall = null;
        boolean autoCommitRestoredToFalse = false;
        private final boolean jobSucceeded;
        private final boolean raiseOnExecute;
        // Simulate a wait that returns no result set, or one with no row, to exercise fail-closed.
        private boolean noResultSet = false;
        private boolean emptyResultSet = false;
        // Simulate the finally-block restore (setAutoCommit(false)) failing.
        private boolean raiseOnRestore = false;
        // Existing-index lookup issued for a skipped IF NOT EXISTS: the bound (table, index name)
        // parameters, and the row it returns (no row when existingIndexFound is false).
        final Map<Integer, String> lookupParams = new HashMap<>();
        private boolean existingIndexFound = false;
        private boolean existingIndexValid = false;
        private String existingIndexJobId = null;
        // Simulate the existing-index lookup itself raising.
        private boolean raiseOnLookup = false;

        // Defaults to a successful build so existing tests (which only assert the CALL was issued)
        // keep passing against the success result set.
        RecordingConnection() {
            this(true);
        }

        RecordingConnection(boolean jobSucceeded) {
            this(jobSucceeded, false);
        }

        RecordingConnection(boolean jobSucceeded, boolean raiseOnExecute) {
            this.jobSucceeded = jobSucceeded;
            this.raiseOnExecute = raiseOnExecute;
        }

        Connection proxy() {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Connection.class}, connHandler());
        }

        private InvocationHandler connHandler() {
            return (proxy, method, args) -> {
                switch (method.getName()) {
                    case "getAutoCommit":
                        return autoCommit;
                    case "setAutoCommit":
                        boolean value = (Boolean) args[0];
                        if (!value && raiseOnRestore) {
                            throw new SQLException("simulated setAutoCommit(false) failure");
                        }
                        autoCommit = value;
                        if (!autoCommit) {
                            autoCommitRestoredToFalse = true;
                        }
                        return null;
                    case "prepareCall":
                        preparedCalls.add((String) args[0]);
                        autoCommitAtPrepareCall = autoCommit;
                        return callableStatementProxy();
                    case "prepareStatement":
                        return lookupStatementProxy();
                    default:
                        return defaultReturn(method);
                }
            };
        }

        private PreparedStatement lookupStatementProxy() {
            InvocationHandler h = (proxy, method, args) -> {
                switch (method.getName()) {
                    case "setString":
                        lookupParams.put((Integer) args[0], (String) args[1]);
                        return null;
                    case "executeQuery":
                        if (raiseOnLookup) {
                            throw new SQLException("simulated lookup failure");
                        }
                        return lookupResultSetProxy();
                    default:
                        return defaultReturn(method);
                }
            };
            return (PreparedStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class}, h);
        }

        // Single (indisvalid, job_id) row, or no row when the index was not found.
        private ResultSet lookupResultSetProxy() {
            boolean[] advanced = {false};
            InvocationHandler h = (proxy, method, args) -> {
                switch (method.getName()) {
                    case "next":
                        if (!existingIndexFound || advanced[0]) return Boolean.FALSE;
                        advanced[0] = true;
                        return Boolean.TRUE;
                    case "getBoolean":
                        if ((Integer) args[0] != 1) throw new SQLException("indisvalid is column 1");
                        return existingIndexValid;
                    case "getString":
                        if ((Integer) args[0] != 2) throw new SQLException("job_id is column 2");
                        return existingIndexJobId;
                    default:
                        return defaultReturn(method);
                }
            };
            return (ResultSet) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{ResultSet.class}, h);
        }

        private CallableStatement callableStatementProxy() {
            InvocationHandler h = (proxy, method, args) -> {
                switch (method.getName()) {
                    case "setString":
                        boundJobIds.add((String) args[1]);
                        return null;
                    case "execute":
                        if (raiseOnExecute) {
                            throw new SQLException("simulated wait_for_job failure");
                        }
                        return !noResultSet;   // false = execute() reports no result set
                    case "getResultSet":
                        return resultSetProxy();
                    default:
                        return defaultReturn(method);
                }
            };
            return (CallableStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{CallableStatement.class}, h);
        }

        // Single-row result set whose boolean column reports whether the build succeeded.
        private ResultSet resultSetProxy() {
            boolean[] advanced = {false};
            InvocationHandler h = (proxy, method, args) -> {
                switch (method.getName()) {
                    case "next":
                        if (emptyResultSet || advanced[0]) return Boolean.FALSE;
                        advanced[0] = true;
                        return Boolean.TRUE;
                    case "getBoolean":
                        return jobSucceeded;
                    default:
                        return defaultReturn(method);
                }
            };
            return (ResultSet) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{ResultSet.class}, h);
        }

        private static Object defaultReturn(Method method) {
            Class<?> r = method.getReturnType();
            if (r == boolean.class) return Boolean.FALSE;
            if (r == int.class) return 0;
            if (r == long.class) return 0L;
            return null;
        }
    }

    private static class FakeDelegate implements SqlScriptExecutor {
        private final List<Results> toReturn;
        FakeDelegate(List<Results> toReturn) { this.toReturn = toReturn; }
        @Override public List<Results> execute(SqlScript sqlScript, Configuration configuration) {
            return toReturn;
        }
    }

    private static Results resultsOf(Result... results) {
        Results r = new Results();
        for (Result result : results) {
            r.addResult(result);
        }
        return r;
    }

    private static Result asyncIndexResult(String jobId, String sql) {
        return new Result(0L, singletonList("job_id"), singletonList(singletonList(jobId)), sql);
    }

    private static Result plainResult(long updateCount, String sql) {
        return new Result(updateCount, emptyList(), emptyList(), sql);
    }

    // What DSQL returns for a skipped IF NOT EXISTS: the job_id column with no rows.
    private static Result skippedIfNotExistsResult(String sql) {
        return new Result(0L, singletonList("job_id"), emptyList(), sql);
    }

    // A skipped IF NOT EXISTS result together with the 42P07 notice DSQL issues for it.
    private static Results skippedWithNotice(String indexName, String sql) {
        Results r = resultsOf(skippedIfNotExistsResult(sql));
        r.addWarning(new WarningImpl(0, "42P07",
                "relation \"" + indexName + "\" already exists, skipping"));
        return r;
    }

    // A Configuration whose dsql extension has awaitAsyncIndexes set as given. FluentConfiguration
    // auto-registers the extension via ServiceLoader, so the wait gate reads the flag from here.
    private static Configuration configWithAwait(boolean await) {
        FluentConfiguration c = new FluentConfiguration();
        c.getPluginRegister().getPlugin(DSQLConfigurationExtension.class).setAwaitAsyncIndexes(await);
        return c;
    }

    @Test
    void waitsForAsyncIndexJob() {
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-123",
                "CREATE INDEX ASYNC idx_orders_customer ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        executor.execute(null, configWithAwait(true));

        assertThat(conn.preparedCalls).containsExactly("CALL sys.wait_for_job(?)");
        assertThat(conn.boundJobIds).containsExactly("job-123");
        // sys.wait_for_job cannot run in a transaction block: the CALL must be issued with
        // autoCommit on, and the original (in-transaction) state restored afterward.
        assertThat(conn.autoCommitAtPrepareCall).isTrue();
        assertThat(conn.autoCommitRestoredToFalse).isTrue();
        assertThat(conn.autoCommit).isFalse();
    }

    @Test
    void doesNotWaitWhenFlagDisabled() {
        // Async-index result with a job_id, but the opt-in flag is off: no wait fires.
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-123",
                "CREATE INDEX ASYNC idx_orders_customer ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(false));
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void doesNotWaitForPlainCreateIndex() {
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(plainResult(0L, "CREATE INDEX idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void doesNotWaitForOrdinaryDml() {
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(plainResult(1L, "INSERT INTO orders VALUES (1)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void failsMigrationForAsyncIndexWithNoJobId() {
        // Async-index statement text but no job_id column/value: with awaitAsyncIndexes on, fail
        // closed rather than silently continuing against a possibly-unbuilt index.
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(plainResult(0L, "CREATE INDEX ASYNC idx ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("no job_id was found");
        assertThat(conn.preparedCalls).isEmpty();
        assertThat(conn.lookupParams).isEmpty();
    }

    @Test
    void acceptsValidExistingIndexWhenIfNotExistsSkips() {
        // Skipped because the index exists and is valid: nothing to wait on, even though its
        // completed build is still listed. Keywords match in any case; the unquoted index name
        // (digits included) is folded to lower case.
        RecordingConnection conn = new RecordingConnection();
        conn.existingIndexFound = true;
        conn.existingIndexValid = true;
        conn.existingIndexJobId = "job-done";
        Results results = resultsOf(skippedIfNotExistsResult(
                "create index async if not exists Idx_Val2 on orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.lookupParams).containsOnly(entry(1, "orders"), entry(2, "idx_val2"));
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void waitsForExistingBuildWhenIfNotExistsSkips() {
        // Skipped while the existing index is still building: wait on that build's job. The
        // comments, which Flyway keeps in the SQL, must not hide the IF NOT EXISTS match. A later
        // CREATE INDEX ASYNC cannot change what the unqualified table refers to, so it still runs.
        RecordingConnection conn = new RecordingConnection();
        conn.existingIndexFound = true;
        conn.existingIndexJobId = "job-existing";
        List<Results> script = Arrays.asList(
                resultsOf(skippedIfNotExistsResult(
                        "-- rerun\nCREATE INDEX ASYNC IF /* c */ NOT EXISTS idx ON orders(customer_id)")),
                resultsOf(asyncIndexResult("job-later", "CREATE INDEX ASYNC idx2 ON orders(x)")));
        new DSQLSqlScriptExecutor(new FakeDelegate(script), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-existing", "job-later");
    }

    @Test
    void failsWhenIfNotExistsSkipsInvalidIndexWithNoJob() {
        // Skipped over an INVALID index whose failed build has been purged from sys.jobs.
        RecordingConnection conn = new RecordingConnection();
        conn.existingIndexFound = true;
        Results results = skippedWithNotice("idx",
                "CREATE INDEX ASYNC IF NOT EXISTS idx ON orders(customer_id)");
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("is INVALID from an earlier failed build")
                .hasMessageContaining("(DSQL: relation \"idx\" already exists, skipping)");
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void failsWhenIfNotExistsSkipsButNoSuchIndexOnTable() {
        // Skipped because the name belongs to another relation (e.g. a table): no index was made.
        // DSQL's own skip notice is carried into the message.
        RecordingConnection conn = new RecordingConnection();
        Results results = skippedWithNotice("orders_lookup",
                "CREATE INDEX ASYNC IF NOT EXISTS orders_lookup ON orders(customer_id)");
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("already used by another object")
                .hasMessageContaining("(DSQL: relation \"orders_lookup\" already exists, skipping)");
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void waitsOnReturnedJobForFreshIfNotExists() {
        // Not skipped: IF NOT EXISTS created the index and returned a job_id, so the wait uses
        // it directly and the existing-index lookup must not run.
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-new",
                "CREATE INDEX ASYNC IF NOT EXISTS idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-new");
        assertThat(conn.lookupParams).isEmpty();
    }

    @Test
    void failsWhenExistingIndexLookupRaises() {
        // A failed lookup must fail the migration, not be read as "nothing to wait on".
        RecordingConnection conn = new RecordingConnection();
        conn.raiseOnLookup = true;
        Results results = resultsOf(skippedIfNotExistsResult(
                "CREATE INDEX ASYNC IF NOT EXISTS idx ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("failed to check the existing index")
                .hasCauseInstanceOf(SQLException.class);
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void looksUpQuotedSchemaQualifiedNamesWhenIfNotExistsSkips() {
        // The table reference is passed through as written (resolved by ::regclass); a quoted
        // index name keeps its case with "" unescaped. Comment markers inside quoted names are
        // kept. A schema-qualified table resolves the same whatever later statements do, so a
        // later SET does not block the check.
        RecordingConnection conn = new RecordingConnection();
        conn.existingIndexFound = true;
        conn.existingIndexValid = true;
        List<Results> script = Arrays.asList(
                resultsOf(skippedIfNotExistsResult("CREATE UNIQUE INDEX ASYNC IF /* actual comment */ NOT EXISTS"
                        + " \"My\"\"Idx/*archive*/\" ON app.\"Orders--archive\" (customer_id)")),
                resultsOf(plainResult(0L, "SET search_path TO other")));
        new DSQLSqlScriptExecutor(new FakeDelegate(script), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.lookupParams)
                .containsOnly(entry(1, "app.\"Orders--archive\""), entry(2, "My\"Idx/*archive*/"));
    }

    @Test
    void failsClosedWhenUnqualifiedTableIsFollowedByAnotherStatement() {
        // The check runs after the script; a later SET search_path (or any other non-index
        // statement) could point the unqualified "orders" at a different table, so fail closed.
        RecordingConnection conn = new RecordingConnection();
        conn.existingIndexFound = true;
        conn.existingIndexValid = true;
        List<Results> script = Arrays.asList(
                resultsOf(skippedIfNotExistsResult(
                        "CREATE INDEX ASYNC IF NOT EXISTS idx ON orders(customer_id)")),
                resultsOf(plainResult(0L, "SET search_path TO other")));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(script), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("Schema-qualify the table");
        assertThat(conn.lookupParams).isEmpty();
    }

    @Test
    void failsClosedWhenIfNotExistsTableCannotBeParsed() {
        // "tablé" must not be cut to "tabl" and checked as another table: no lookup, missingJobId.
        RecordingConnection conn = new RecordingConnection();
        conn.existingIndexFound = true;
        conn.existingIndexValid = true;
        Results results = resultsOf(skippedIfNotExistsResult(
                "CREATE INDEX ASYNC IF NOT EXISTS idx_x ON tablé (c)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("no job_id was found");
        assertThat(conn.lookupParams).isEmpty();
    }

    @Test
    void waitsForAsyncIndexWhenJobIdColumnIsUppercase() {
        // The job_id column match is case-insensitive; a JOB_ID column must still drive the wait.
        RecordingConnection conn = new RecordingConnection();
        Result result = new Result(0L, singletonList("JOB_ID"),
                singletonList(singletonList("job-uc")),
                "CREATE INDEX ASYNC idx ON orders(customer_id)");
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(resultsOf(result))), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-uc");
    }

    @Test
    void failsMigrationForAsyncIndexWithoutJobIdColumn() {
        // Async-index statement whose result has a row but no job_id column: fail closed rather
        // than silently continuing against a possibly-unbuilt index.
        RecordingConnection conn = new RecordingConnection();
        Result result = new Result(0L, singletonList("other"),
                singletonList(singletonList("val")),
                "CREATE INDEX ASYNC idx ON orders(customer_id)");
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(resultsOf(result))), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class);
        assertThat(conn.preparedCalls).isEmpty();
    }

    @Test
    void waitsForUniqueAsyncIndex() {
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-u",
                "CREATE UNIQUE INDEX ASYNC idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-u");
    }

    @Test
    void waitsForAsyncIndexWithLeadingLineComment() {
        // Flyway keeps a leading "-- ..." comment in Result.sql(); the wait must still fire.
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-lc",
                "-- add lookup index\nCREATE INDEX ASYNC idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-lc");
    }

    @Test
    void waitsForAsyncIndexWithLeadingBlockComment() {
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-bc",
                "/* add lookup index */ CREATE INDEX ASYNC idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-bc");
    }

    @Test
    void waitsForAsyncIndexWithOverlappingBlockCommentDelimiter() {
        // "/*/" must be treated as an unterminated opener, not a complete comment: the closer is
        // searched past the two-char opener so the whole "/*/ ... */" prefix is stripped.
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-oc",
                "/*/ note */ CREATE INDEX ASYNC idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-oc");
    }

    @Test
    void waitsForAsyncIndexWithCommentBetweenKeywords() {
        // A comment sitting between the keywords must not hide the match.
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-bk",
                "CREATE /* c1 */ INDEX /* c2 */ ASYNC idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-bk");
    }

    @Test
    void waitsForAsyncIndexWithNestedBlockComment() {
        // A nested block comment must be fully consumed, not truncated at the first "*/".
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(asyncIndexResult("job-nc",
                "/* outer /* inner */ still comment */ CREATE INDEX ASYNC idx ON orders(customer_id)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-nc");
    }

    @Test
    void skipsNullResultsEntry() {
        // Flyway 11.9 adds a null Results when flushing an empty batch around unbatchable DDL; the
        // decorator must skip it and still wait on the following async-index result.
        RecordingConnection conn = new RecordingConnection();
        List<Results> delegateResults = new ArrayList<>();
        delegateResults.add(null);
        delegateResults.add(resultsOf(asyncIndexResult("job-nb",
                "CREATE INDEX ASYNC idx ON orders(customer_id)")));
        new DSQLSqlScriptExecutor(new FakeDelegate(delegateResults), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-nb");
    }

    @Test
    void waitsOncePerAsyncIndexResultInOrder() {
        RecordingConnection conn = new RecordingConnection();
        Results results = resultsOf(
                asyncIndexResult("job-1", "CREATE INDEX ASYNC a ON t(x)"),
                plainResult(0L, "CREATE INDEX b ON t(y)"),
                asyncIndexResult("job-2", "CREATE INDEX ASYNC c ON t(z)"));
        new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy())
                .execute(null, configWithAwait(true));
        assertThat(conn.boundJobIds).containsExactly("job-1", "job-2");
    }

    @Test
    void passesDelegateResultsThroughUnchanged() {
        RecordingConnection conn = new RecordingConnection();
        List<Results> delegateResults = singletonList(resultsOf(plainResult(0L, "SELECT 1")));
        List<Results> returned =
                new DSQLSqlScriptExecutor(new FakeDelegate(delegateResults), conn.proxy())
                        .execute(null, configWithAwait(true));
        assertThat(returned).isSameAs(delegateResults);
    }

    @Test
    void failsMigrationWhenAsyncIndexBuildReturnsFalse() {
        // A failed build does not raise: sys.wait_for_job returns a single boolean 'false'. The
        // wait must still be issued, and the migration must fail explicitly on the false result.
        RecordingConnection conn = new RecordingConnection(false);
        Results results = resultsOf(asyncIndexResult("job-fail",
                "CREATE INDEX ASYNC idx_orders_customer ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class);
        assertThat(conn.preparedCalls).containsExactly("CALL sys.wait_for_job(?)");
        assertThat(conn.boundJobIds).containsExactly("job-fail");
        // autoCommit must be restored even when the migration fails (the restore is in finally).
        assertThat(conn.autoCommitRestoredToFalse).isTrue();
        assertThat(conn.autoCommit).isFalse();
    }

    @Test
    void failsMigrationWhenWaitRaisesSqlException() {
        // Belt-and-suspenders: if sys.wait_for_job raises instead, it still fails the migration.
        RecordingConnection conn = new RecordingConnection(true, true);
        Results results = resultsOf(asyncIndexResult("job-raise",
                "CREATE INDEX ASYNC idx_orders_customer ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class);
        assertThat(conn.preparedCalls).containsExactly("CALL sys.wait_for_job(?)");
        // autoCommit must be restored even when the wait raises (the restore is in finally).
        assertThat(conn.autoCommitRestoredToFalse).isTrue();
        assertThat(conn.autoCommit).isFalse();
    }

    @Test
    void failsMigrationWhenAutoCommitRestoreFailsAfterSuccessfulWait() {
        // Body succeeds but the finally restore throws: must not be swallowed (a leaked
        // autoCommit=true would silently defeat rollback for the rest of the run).
        RecordingConnection conn = new RecordingConnection();
        conn.raiseOnRestore = true;
        Results results = resultsOf(asyncIndexResult("job-restore",
                "CREATE INDEX ASYNC idx ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("restore autoCommit");
    }

    @Test
    void keepsPrimaryFailureWhenAutoCommitRestoreAlsoFails() {
        // Body fails (build did not succeed) AND the restore throws: the build failure stays the
        // primary error, with the restore failure attached as suppressed.
        RecordingConnection conn = new RecordingConnection(false);
        conn.raiseOnRestore = true;
        Results results = resultsOf(asyncIndexResult("job-both",
                "CREATE INDEX ASYNC idx ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class)
                .hasMessageContaining("did not succeed")
                .satisfies(t -> assertThat(t.getSuppressed()).isNotEmpty());
    }

    @Test
    void failsMigrationWhenWaitReturnsNoResultSet() {
        // execute() reporting no result set is not a success confirmation: fail closed.
        RecordingConnection conn = new RecordingConnection();
        conn.noResultSet = true;
        Results results = resultsOf(asyncIndexResult("job-nrs",
                "CREATE INDEX ASYNC idx ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class);
        assertThat(conn.autoCommitRestoredToFalse).isTrue();
    }

    @Test
    void failsMigrationWhenWaitReturnsEmptyResultSet() {
        // A result set with no row is not a success confirmation either: fail closed.
        RecordingConnection conn = new RecordingConnection();
        conn.emptyResultSet = true;
        Results results = resultsOf(asyncIndexResult("job-ers",
                "CREATE INDEX ASYNC idx ON orders(customer_id)"));
        DSQLSqlScriptExecutor executor =
                new DSQLSqlScriptExecutor(new FakeDelegate(singletonList(results)), conn.proxy());

        assertThatThrownBy(() -> executor.execute(null, configWithAwait(true)))
                .isInstanceOf(FlywayException.class);
        assertThat(conn.autoCommitRestoredToFalse).isTrue();
    }
}
