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
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                    default:
                        return defaultReturn(method);
                }
            };
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
                .isInstanceOf(FlywayException.class);
        assertThat(conn.preparedCalls).isEmpty();
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
