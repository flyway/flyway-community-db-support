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
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.database.postgresql.PostgreSQLConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Aurora DSQL connection implementation for Flyway.
 *
 * <p>Skips {@code SET ROLE} restoration (DSQL uses IAM authentication) and bypasses
 * advisory locks (DSQL uses optimistic concurrency control).
 */
@CustomLog
public class DSQLConnection extends PostgreSQLConnection {

    public DSQLConnection(DSQLDatabase database, Connection connection) {
        super(database, connection);
    }

    @Override
    protected void doRestoreOriginalState() throws SQLException {
        LOG.debug("Skipping SET ROLE restoration (not supported by Aurora DSQL)");
    }

    @Override
    public Schema getSchema(String name) {
        return new DSQLSchema(jdbcTemplate, (DSQLDatabase) database, name);
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        LOG.debug("Executing without advisory lock (not supported by Aurora DSQL)");
        try {
            T result = callable.call();
            reconcileDuplicateHistoryRows(table);
            return result;
        } catch (SQLException e) {
            throw new FlywaySqlException("Unable to execute migration", e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Unable to execute migration", e);
        }
    }

    /**
     * Removes duplicate schema-history rows left by an OCC retry.
     *
     * <p>DSQL keeps the migration's schema-history insert on the auto-commit main connection, so it
     * commits before the migration transaction's own commit. When that commit loses the OCC race and
     * {@link DSQLExecutionTemplate} replays the migration, a second history row is written for the
     * same version — leaving the earlier attempt's row orphaned (a lower {@code installed_rank}).
     * See {@link #reconcileStatement(String)} for what is deleted.
     *
     * <p>Best-effort and idempotent: a no-op when there are no duplicates, and never fails the
     * migration if cleanup itself errors. The schema-creation marker, which carries no version, is
     * left untouched.
     */
    private void reconcileDuplicateHistoryRows(Table table) {
        try {
            // lock() also wraps the history-table create/drop; skip those (e.g. after clean drops
            // the table) so the DELETE does not run against a missing table. The main connection is
            // already auto-commit, so the DELETE commits on its own.
            if (!table.exists()) {
                return;
            }
            jdbcTemplate.execute(reconcileStatement(table.toString()));
        } catch (Exception e) {
            // Best-effort: never fail an already-succeeded migration. Broad catch because
            // table.exists() rethrows its SQLException unchecked.
            LOG.warn("Could not reconcile duplicate Aurora DSQL schema-history rows: " + e.getMessage());
        }
    }

    /**
     * Deletes lower-ranked successful rows duplicating a higher-ranked successful row of the same
     * version <em>and type</em> — the shape an OCC retry leaves.
     *
     * <p>Type is compared because Flyway encodes state as same-version rows that are not duplicates:
     * {@code repair} appends a {@code DELETE} marker and {@code undo} its own entry <em>above</em> the
     * row they refer to, so matching on version alone would delete that row.
     *
     * <p>Success is compared on both sides because a migration that exhausts its retries records a
     * failed row above the successful ones (DSQL records failures, having no DDL transactions).
     * Taking {@code MAX} over successful rows only keeps that failed row from making every
     * successful row below it look superseded.
     */
    static String reconcileStatement(String table) {
        return "DELETE FROM " + table + " h"
                + " WHERE h.\"version\" IS NOT NULL"
                + " AND h.\"success\" = TRUE"
                + " AND h.\"installed_rank\" < ("
                + "SELECT MAX(h2.\"installed_rank\") FROM " + table + " h2"
                + " WHERE h2.\"version\" = h.\"version\""
                + " AND h2.\"type\" = h.\"type\""
                + " AND h2.\"success\" = TRUE)";
    }
}
