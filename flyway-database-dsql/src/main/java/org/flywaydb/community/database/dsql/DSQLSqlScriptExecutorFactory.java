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

import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;

import java.sql.Connection;

/**
 * Wraps the default {@link SqlScriptExecutorFactory} so each SQL-script executor can block on
 * Aurora DSQL {@code CREATE INDEX ASYNC} builds when {@code flyway.dsql.awaitAsyncIndexes} is
 * enabled (see {@link DSQLSqlScriptExecutor}). The factory always wraps; the blocking itself is
 * opt-in and off by default.
 *
 * <p>Kept separate from the executor so it can be unit-tested with a fake delegate factory: the
 * base factory closes over a {@code JdbcConnectionFactory} whose constructor opens a live
 * connection, so the real one cannot be built with nulls.
 */
class DSQLSqlScriptExecutorFactory implements SqlScriptExecutorFactory {

    private final SqlScriptExecutorFactory delegate;

    DSQLSqlScriptExecutorFactory(SqlScriptExecutorFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public SqlScriptExecutor createSqlScriptExecutor(Connection connection, boolean undo,
                                                     boolean batch, boolean outputQueryResults) {
        SqlScriptExecutor delegateExecutor =
                delegate.createSqlScriptExecutor(connection, undo, batch, outputQueryResults);
        return new DSQLSqlScriptExecutor(delegateExecutor, connection);
    }
}
