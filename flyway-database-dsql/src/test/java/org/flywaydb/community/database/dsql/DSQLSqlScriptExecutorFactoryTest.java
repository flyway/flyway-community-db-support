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

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.sqlscript.SqlScript;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutor;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

class DSQLSqlScriptExecutorFactoryTest {

    private static final SqlScriptExecutor STUB_EXECUTOR = new SqlScriptExecutor() {
        @Override public List<Results> execute(SqlScript sqlScript, Configuration configuration) {
            return emptyList();
        }
    };

    @Test
    void wrapsDelegateExecutorWithDsqlExecutor() {
        SqlScriptExecutorFactory delegate =
                (connection, undo, batch, outputQueryResults) -> STUB_EXECUTOR;
        DSQLSqlScriptExecutorFactory factory = new DSQLSqlScriptExecutorFactory(delegate);

        SqlScriptExecutor executor = factory.createSqlScriptExecutor(null, false, false, false);

        assertThat(executor).isInstanceOf(DSQLSqlScriptExecutor.class);
    }
}
