/*-
 * ========================LICENSE_START=================================
 * flyway-database-db2zos
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
package org.flywaydb.community.database.db2z;

import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.resource.StringResource;
import org.flywaydb.core.internal.sqlscript.SqlStatement;
import org.flywaydb.core.internal.sqlscript.SqlStatementIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DB2ZParserTest {

    private List<SqlStatement> parse(String sql) {
        DB2ZParser parser = new DB2ZParser(new FluentConfiguration(), new ParsingContext());
        List<SqlStatement> statements = new ArrayList<>();
        try (SqlStatementIterator iterator = parser.parse(new StringResource(sql))) {
            while (iterator.hasNext()) {
                statements.add(iterator.next());
            }
        }
        return statements;
    }

    @Test
    public void parsesSimpleCallStatement() {
        List<SqlStatement> statements = parse("CALL MYSCHEMA.MYPROC('param1', 123);");

        assertThat(statements).hasSize(1);
        DB2ZCallProcedureParsedStatement call = (DB2ZCallProcedureParsedStatement) statements.get(0);
        assertThat(call.getProcedureName()).isEqualTo("MYSCHEMA.MYPROC");
        assertThat(call.getParms()).containsExactly("param1", 123);
    }

    @Test
    public void parsesCallStatementWithStringIntegerAndNullParameters() {
        List<SqlStatement> statements = parse("CALL MYSCHEMA.MYPROC('it''s a test', 42, NULL);");

        DB2ZCallProcedureParsedStatement call = (DB2ZCallProcedureParsedStatement) statements.get(0);
        assertThat(call.getParms()).containsExactly("it's a test", 42, null);
    }

    @Test
    public void parsesIndentedCallStatement() {
        List<SqlStatement> statements = parse("   CALL MYSCHEMA.MYPROC('param1', 123);");

        DB2ZCallProcedureParsedStatement call = (DB2ZCallProcedureParsedStatement) statements.get(0);
        assertThat(call.getProcedureName()).isEqualTo("MYSCHEMA.MYPROC");
        assertThat(call.getParms()).containsExactly("param1", 123);
    }

    // Regression: this used to parse correctly before the DB2Z_CALL_WITH_PARMS_REGEX change
    // in this PR (anchoring with ^/$ + MULTILINE). A comment line pushed down an indented CALL
    // no longer matches, so the statement silently loses its parameters.
    @Test
    public void parsesCallStatementPrecededByCommentAndIndented() {
        List<SqlStatement> statements = parse("-- archive old records\n   CALL MYSCHEMA.MYPROC('param1', 123);");

        assertThat(statements.get(0)).isInstanceOf(DB2ZCallProcedureParsedStatement.class);
        DB2ZCallProcedureParsedStatement call = (DB2ZCallProcedureParsedStatement) statements.get(0);
        assertThat(call.getProcedureName()).isEqualTo("MYSCHEMA.MYPROC");
        assertThat(call.getParms()).containsExactly("param1", 123);
    }

    // This test case contains a CALL statemwnt which is preceded by a commented CALL statement
    @Test
    public void parsesCallStatementPrecededByCommentedCall() {
        List<SqlStatement> statements = parse(
                "-- CALL SYSPROC.DSNUTILU('<To be ignored>');\n\n"
                + "-- commented out for test reasons\n"
                + "   CALL MYSCHEMA.MYPROC('param1', 123);"
                );

        assertThat(statements.get(0)).isInstanceOf(DB2ZCallProcedureParsedStatement.class);
        DB2ZCallProcedureParsedStatement call = (DB2ZCallProcedureParsedStatement) statements.get(0);
        assertThat(call.getProcedureName()).isEqualTo("MYSCHEMA.MYPROC");
        assertThat(call.getParms()).containsExactly("param1", 123);
    }

    // This test case contains a CALL with a long parameter list wrapped across multiple lines, e.g. the
    // multi-param SYSPROC.DSNUTILU utility calls this class already has special-case handling for
    // below. It fails to bind parameters both before and after this PR's regex change, since `.`
    // still isn't DOTALL and can't cross the line breaks inside the parentheses.
    @Test
    public void parsesCallStatementWithParametersSplitAcrossLines() {
        List<SqlStatement> statements = parse(
                "CALL SYSPROC.DSNUTILU(\n"
              + "    'DB2Z.UTILPROC',\n"
              + "    'LOAD.SYSIN.DATASET',\n"
              + "    'LOAD.UTPRINT.DATASET',\n"
              + "    ' ',\n"
              + "    'S',\n"
              + "    0\n"
              + ");");

        assertThat(statements.get(0)).isInstanceOf(DB2ZCallProcedureParsedStatement.class);
        DB2ZCallProcedureParsedStatement call = (DB2ZCallProcedureParsedStatement) statements.get(0);
        assertThat(call.getProcedureName()).isEqualTo("SYSPROC.DSNUTILU");
        assertThat(call.getParms()).containsExactly(
                "DB2Z.UTILPROC", "LOAD.SYSIN.DATASET", "LOAD.UTPRINT.DATASET", " ", "S", 0);
    }
}
