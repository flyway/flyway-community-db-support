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
import org.flywaydb.core.api.callback.Warning;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
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
 * that still succeeds. A rerun must drop it first.
 *
 * <p>{@code CREATE INDEX ASYNC IF NOT EXISTS} returns no {@code job_id} when it is skipped because
 * the name is already taken. The existing index on the named table is then checked instead: a
 * valid index passes, one still building is awaited through its {@code sys.jobs} entry, and an
 * {@code INVALID} one (or no such index, e.g. the name belongs to a table) fails the migration.
 * The check runs after the whole script, so an unqualified table is only checked when every later
 * statement is a {@code CREATE INDEX ASYNC}; any other later statement could change which table
 * the name refers to, so the migration fails and asks for a schema-qualified table instead.
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

    // A quoted ("..." with "" escapes) or unquoted ASCII SQL identifier.
    private static final String IDENT = "(?:\"(?:[^\"]|\"\")+\"|[A-Za-z_][A-Za-z0-9_$]*)";

    // Captures the index name and the (optionally schema-qualified) table of an IF NOT EXISTS
    // statement, so a skipped statement can be checked against the index that already exists.
    // Group 3 is set only for a schema-qualified table. The table must be followed by its column
    // list, so a partial match fails closed.
    private static final Pattern CREATE_INDEX_ASYNC_IF_NOT_EXISTS = Pattern.compile(
            "^\\s*CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+ASYNC\\s+IF\\s+NOT\\s+EXISTS\\s+(" + IDENT
                    + ")\\s+ON\\s+(" + IDENT + "(\\." + IDENT + ")?)(?=\\s*\\()",
            Pattern.CASE_INSENSITIVE);

    // Validity of the named index on the named table, plus the job_id of its latest build (null
    // once sys.jobs has purged it). No row means no such index exists on that table.
    // Cast to name so an index name over 63 bytes is shortened as DSQL shortened it.
    private static final String EXISTING_INDEX_QUERY =
            "SELECT i.indisvalid, (SELECT j.job_id FROM sys.jobs j WHERE j.object_id = i.indexrelid"
                    + " ORDER BY j.start_time DESC LIMIT 1)"
                    + " FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid"
                    + " WHERE i.indrelid = ?::regclass AND c.relname = ?::name";

    // SQLSTATE of the notice DSQL issues when IF NOT EXISTS skips a statement.
    private static final String RELATION_EXISTS_STATE = "42P07";

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
        // Position of the last statement that is not a CREATE INDEX ASYNC, for the unqualified-
        // table check in asyncIndexJobId.
        int lastOther = -1;
        int pos = 0;
        for (Results results : resultsList) {
            if (results == null) {
                continue;
            }
            for (Result result : results.getResults()) {
                if (!isCreateIndexAsync(stripComments(result.sql()))) {
                    lastOther = pos;
                }
                pos++;
            }
        }
        pos = 0;
        for (Results results : resultsList) {
            // Flyway 11.9 adds a null Results when flushing an empty batch around an unbatchable
            // statement (CREATE INDEX ASYNC under flyway.batch=true).
            if (results == null) {
                continue;
            }
            for (Result result : results.getResults()) {
                String jobId = asyncIndexJobId(result, results.getWarnings(), lastOther > pos);
                if (jobId != null) {
                    waitForJob(jobId);
                }
                pos++;
            }
        }
        return resultsList;
    }

    private static boolean isCreateIndexAsync(String strippedSql) {
        return strippedSql != null && CREATE_INDEX_ASYNC.matcher(strippedSql).find();
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

    /**
     * Returns the {@code job_id} to wait on for this result, or null if there is nothing to wait
     * on (not an async-index statement, or a skipped IF NOT EXISTS over a valid index).
     * {@code laterOtherStatement} is whether a statement other than CREATE INDEX ASYNC follows it
     * in the script; such a statement could change which table an unqualified name refers to by
     * the time the check runs.
     */
    private String asyncIndexJobId(Result result, List<Warning> warnings,
                                   boolean laterOtherStatement) {
        String sql = stripComments(result.sql());
        if (!isCreateIndexAsync(sql)) {
            return null;
        }
        String jobId = jobIdColumnValue(result);
        if (jobId != null) {
            return jobId;
        }
        Matcher ifNotExists = CREATE_INDEX_ASYNC_IF_NOT_EXISTS.matcher(sql);
        if (ifNotExists.find()) {
            String indexName = ifNotExists.group(1);
            String table = ifNotExists.group(2);
            if (ifNotExists.group(3) == null && laterOtherStatement) {
                throw new FlywayException("Aurora DSQL index " + indexName + " on " + table
                        + " cannot be checked: IF NOT EXISTS skipped it" + skipNotice(warnings)
                        + ", and a later statement in this script may change which table " + table
                        + " refers to. Schema-qualify the table.");
            }
            return existingIndexJobId(sql, indexName, table, warnings);
        }
        throw missingJobId(sql);
    }

    /** Returns the first row's {@code job_id} value, or null if the result carries none. */
    private static String jobIdColumnValue(Result result) {
        List<String> columns = result.columns();
        List<List<String>> data = result.data();
        if (columns == null || data == null || data.isEmpty()) {
            return null;
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
            return null;
        }
        String jobId = firstRow.get(idx);
        return (jobId == null || jobId.isEmpty()) ? null : jobId;
    }

    /**
     * Checks the index a skipped IF NOT EXISTS left in place: returns null when it is valid,
     * the job_id of its build when it is not yet valid (waitForJob then passes or fails on the
     * build's outcome), and fails when it is INVALID with no build left to wait on, or when no
     * index of that name exists on the table (the name belongs to another relation).
     */
    private String existingIndexJobId(String sql, String indexName, String table,
                                      List<Warning> warnings) {
        try (PreparedStatement ps = connection.prepareStatement(EXISTING_INDEX_QUERY)) {
            ps.setString(1, table);
            ps.setString(2, unquoteIdentifier(indexName));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new FlywayException("Aurora DSQL index " + indexName + " was not created on "
                            + table + ": IF NOT EXISTS skipped it because the name is already used by"
                            + " another object" + skipNotice(warnings) + ". Use a different index name.");
                }
                if (rs.getBoolean(1)) {
                    LOG.debug("Aurora DSQL async index already exists and is valid: " + sql);
                    return null;
                }
                String jobId = rs.getString(2);
                if (jobId == null) {
                    throw new FlywayException("Aurora DSQL index " + indexName + " on " + table
                            + " is INVALID from an earlier failed build, so IF NOT EXISTS cannot reuse"
                            + " it" + skipNotice(warnings) + ". Drop the index and rerun.");
                }
                return jobId;
            }
        } catch (SQLException e) {
            throw new FlywayException("Aurora DSQL async index build could not be awaited: failed "
                    + "to check the existing index for: " + sql, e);
        }
    }

    /**
     * Formats DSQL's own skip notice (SQLSTATE 42P07, "relation ... already exists, skipping") for
     * an error message, or returns "" when the statement carried none.
     */
    private static String skipNotice(List<Warning> warnings) {
        if (warnings != null) {
            for (Warning warning : warnings) {
                if (RELATION_EXISTS_STATE.equals(warning.getState())) {
                    return " (DSQL: " + warning.getMessage() + ")";
                }
            }
        }
        return "";
    }

    /** Folds an unquoted identifier to lower case; unquotes a quoted one ("" becomes "). */
    private static String unquoteIdentifier(String ident) {
        if (ident.startsWith("\"")) {
            return ident.substring(1, ident.length() - 1).replace("\"\"", "\"");
        }
        return ident.toLowerCase(Locale.ROOT);
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
     * Quoted identifiers are copied as is, so comment markers inside them are kept; string literals
     * are not tracked, as none can appear before the table's column list that is matched.
     */
    private static String stripComments(String sql) {
        if (sql == null) {
            return null;
        }
        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        int n = sql.length();
        while (i < n) {
            if (sql.charAt(i) == '"') {
                // A doubled "" escape just closes and reopens the identifier.
                int close = sql.indexOf('"', i + 1);
                int end = (close < 0) ? n : close + 1;
                out.append(sql, i, end);
                i = end;
            } else if (sql.charAt(i) == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
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
