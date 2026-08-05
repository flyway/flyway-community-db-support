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

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Blocks until an Aurora DSQL {@code CREATE INDEX ASYNC} build completes.
 *
 * <p>DSQL has no synchronous index creation: {@code CREATE INDEX ASYNC} returns a runtime
 * {@code job_id} and builds the index in the background. Static SQL cannot thread that id into
 * the required {@code CALL sys.wait_for_job('<job_id>')}, so a plain SQL migration reports
 * success while the index is still building. When {@code flyway.dsql.awaitAsyncIndexes} is true,
 * this decorator runs the delegate executor, then for every result that is a
 * {@code CREATE INDEX ASYNC} carrying a {@code job_id} issues the wait on the same connection
 * before returning — so a later migration step sees a built index. Per the DSQL docs (and verified
 * against a live cluster), a build that does not succeed does not raise: {@code sys.wait_for_job}
 * returns a single boolean row that is {@code false} when the build failed or the wait timed out,
 * so the outcome is detected by inspecting that boolean and the migration is failed explicitly (a
 * raised {@link SQLException} is still handled as a fallback).
 *
 * <p>A failed build leaves an {@code INVALID} index in place (the DSQL docs recommend dropping and
 * recreating it); the migration fails without dropping it, since a timed-out wait may front a build
 * that still succeeds. A rerun must drop it first — {@code IF NOT EXISTS} can silently accept the
 * {@code INVALID} index.
 *
 * <p>Opt-in, off by default: the wait fires only when {@code flyway.dsql.awaitAsyncIndexes} is
 * enabled. When disabled (the default), the decorator is a pass-through that returns the
 * delegate's results untouched.
 *
 * <p>SQL migrations only: Java migrations run against {@code context.getConnection()} directly and
 * bypass this executor, managing their own waiting.
 */
@CustomLog
class DSQLSqlScriptExecutor implements SqlScriptExecutor {

    // Matches a CREATE INDEX ... ASYNC statement (tolerant of UNIQUE, whitespace). The statement
    // text is passed through stripComments() first, which replaces every comment with a space, so
    // comments Flyway keeps in Result.sql() (leading, or between the keywords) cannot hide a match.
    private static final Pattern CREATE_INDEX_ASYNC = Pattern.compile(
            "^\\s*CREATE\\s+(UNIQUE\\s+)?INDEX\\s+ASYNC\\b", Pattern.CASE_INSENSITIVE);

    private static final String JOB_ID_COLUMN = "job_id";

    private final SqlScriptExecutor delegate;
    private final Connection connection;

    DSQLSqlScriptExecutor(SqlScriptExecutor delegate, Connection connection) {
        this.delegate = delegate;
        this.connection = connection;
    }

    @Override
    public List<Results> execute(SqlScript sqlScript, Configuration configuration) {
        List<Results> resultsList = delegate.execute(sqlScript, configuration);
        if (!awaitAsyncIndexesEnabled(configuration)) {
            return resultsList;
        }
        for (Results results : resultsList) {
            // Flyway 11.9 adds a null Results when flushing an empty batch around an unbatchable
            // statement (CREATE INDEX ASYNC under flyway.batch=true).
            if (results == null) {
                continue;
            }
            for (Result result : results.getResults()) {
                String jobId = asyncIndexJobId(result);
                if (jobId != null) {
                    waitForJob(jobId);
                }
            }
        }
        return resultsList;
    }

    /**
     * Whether the opt-in async-index wait is enabled for this run. Reads the {@code dsql} extension
     * from the {@link Configuration} handed to this call; null-guarded to {@code false} so a missing
     * configuration or extension leaves the decorator as a pass-through.
     */
    private static boolean awaitAsyncIndexesEnabled(Configuration configuration) {
        if (configuration == null || configuration.getPluginRegister() == null) {
            return false;
        }
        DSQLConfigurationExtension ext =
                configuration.getPluginRegister().getPlugin(DSQLConfigurationExtension.class);
        return ext != null && ext.isAwaitAsyncIndexes();
    }

    /** Returns the async-index {@code job_id} for this result, or null if it is not one. */
    private static String asyncIndexJobId(Result result) {
        String sql = stripComments(result.sql());
        if (sql == null || !CREATE_INDEX_ASYNC.matcher(sql).find()) {
            return null;
        }
        List<String> columns = result.columns();
        List<List<String>> data = result.data();
        if (columns == null || data == null || data.isEmpty()) {
            throw missingJobId(sql);
        }
        int idx = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (JOB_ID_COLUMN.equalsIgnoreCase(columns.get(i))) {
                idx = i;
                break;
            }
        }
        List<String> firstRow = data.get(0);
        if (idx < 0 || idx >= firstRow.size()) {
            throw missingJobId(sql);
        }
        String jobId = firstRow.get(idx);
        if (jobId == null || jobId.isEmpty()) {
            throw missingJobId(sql);
        }
        return jobId;
    }

    /**
     * Fails the migration when an async-index statement matched but no {@code job_id} was
     * capturable: with awaitAsyncIndexes enabled the wait must not be silently skipped, since a
     * later step could run against an unbuilt index.
     */
    private static FlywayException missingJobId(String sql) {
        return new FlywayException("Aurora DSQL async index build could not be awaited: "
                + "awaitAsyncIndexes is enabled and this statement matched CREATE INDEX ASYNC, but "
                + "no job_id was found in its result: " + sql);
    }

    /**
     * Replaces every line ({@code -- ...}) and block ({@code /}{@code * ... *}{@code /}, nested)
     * comment with a single space, so a comment cannot hide the {@code CREATE INDEX ASYNC} keywords
     * whether it precedes them or sits between them. Flyway retains such comments in
     * {@link Result#sql()}. Each comment becomes a space (not nothing) to keep keyword boundaries.
     */
    private static String stripComments(String sql) {
        if (sql == null) {
            return null;
        }
        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        int n = sql.length();
        while (i < n) {
            if (sql.charAt(i) == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int nl = sql.indexOf('\n', i + 2);
                i = (nl < 0) ? n : nl + 1;
                out.append(' ');
            } else if (sql.charAt(i) == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int depth = 1;
                i += 2;
                while (i < n && depth > 0) {
                    if (sql.charAt(i) == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                        depth++;
                        i += 2;
                    } else if (sql.charAt(i) == '*' && i + 1 < n && sql.charAt(i + 1) == '/') {
                        depth--;
                        i += 2;
                    } else {
                        i++;
                    }
                }
                out.append(' ');
            } else {
                out.append(sql.charAt(i++));
            }
        }
        return out.toString().strip();
    }

    private void waitForJob(String jobId) {
        boolean restoreAutoCommit = false;
        RuntimeException primary = null;
        try {
            // sys.wait_for_job cannot run inside a transaction block. Flyway hands this executor a
            // connection with an open transaction, so switch to autocommit (which commits the
            // pending transaction) before the CALL, mirroring DSQLSchema.doClean(). Switching to
            // autocommit is what commits the CREATE INDEX ASYNC that produced this job_id (it was
            // still open in the migration's transaction), so the index exists before we wait on it.
            // If this commit raises OCC, the DDL did not commit and DSQLExecutionTemplate replaying
            // the migration is correct. The wait itself (a status read on sys.jobs) writes nothing,
            // so it does not raise OCC — hence no local retry that would replay the committed DDL.
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
                restoreAutoCommit = true;
            }
            try (CallableStatement cs = connection.prepareCall("CALL sys.wait_for_job(?)")) {
                cs.setString(1, jobId);
                boolean hasResultSet = cs.execute();   // blocks server-side until the job finishes or times out
                // sys.wait_for_job returns a single boolean: true = succeeded, false = the build
                // did not succeed (it failed, or the wait timed out). A non-success does NOT raise
                // (per the DSQL docs and verified live), so fail closed unless a true row is seen.
                boolean succeeded = false;
                if (hasResultSet) {
                    try (ResultSet rs = cs.getResultSet()) {
                        succeeded = rs.next() && rs.getBoolean(1);
                    }
                }
                if (!succeeded) {
                    throw new FlywayException(
                            "Aurora DSQL async index build did not succeed (job_id=" + jobId
                                    + "); it failed or the wait timed out — see sys.jobs for"
                                    + " status and details");
                }
                LOG.debug("Waited for Aurora DSQL async index build, job_id=" + jobId);
            }
        } catch (SQLException e) {
            primary = new FlywayException(
                    "Aurora DSQL async index build failed or could not be awaited (job_id="
                            + jobId + ")", e);
        } catch (RuntimeException e) {
            primary = e;
        } finally {
            if (restoreAutoCommit) {
                try {
                    connection.setAutoCommit(false);
                } catch (SQLException e) {
                    // A failed restore leaves the connection non-transactional for the rest of the
                    // run, silently defeating rollback: never swallow it. Attach to any in-flight
                    // error so that primary isn't masked; otherwise it becomes the failure.
                    if (primary != null) {
                        primary.addSuppressed(e);
                    } else {
                        primary = new FlywayException("Failed to restore autoCommit after Aurora "
                                + "DSQL async index wait (job_id=" + jobId + ")", e);
                    }
                }
            }
        }
        if (primary != null) {
            throw primary;
        }
    }
}
