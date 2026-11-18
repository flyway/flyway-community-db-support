/*-
 * ========================LICENSE_START=================================
 * flyway-database-polardb
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
package org.flywaydb.community.database.polardb;

import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.resource.StringResource;
import org.flywaydb.core.internal.sqlscript.SqlStatement;
import org.flywaydb.core.internal.sqlscript.SqlStatementIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PolarDBOracleParserTest {
    @Test
    void parsesOracleStyleProcedureWithoutSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.add_log(\n"
                        + "    p_id IN INTEGER,\n"
                        + "    p_message IN VARCHAR2\n"
                        + ") AS\n"
                        + "BEGIN\n"
                        + "    INSERT INTO app.logs VALUES (\n"
                        + "        p_id, q'[semi;colon]', p_message);\n"
                        + "END;\n"
                        + "CALL app.add_log(1, 'ok');\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("q'[semi;colon]'")
                .contains("END")
                .doesNotContain("CALL app.add_log");
        assertThat(statements.get(1).getSql())
                .startsWith("CALL app.add_log");
    }

    @Test
    void parsesProcedureWithDeclarationsBeforeBegin() {
        List<SqlStatement> statements = parse(
                "/* Procedure with local declarations. */\n"
                        + "CREATE OR REPLACE PROCEDURE "
                        + "app.insert_search_result(\n"
                        + "    p_id IN INTEGER,\n"
                        + "    p_code OUT VARCHAR2\n"
                        + ") IS\n"
                        + "    procedure_step VARCHAR2(500);\n"
                        + "    output_code VARCHAR2(10);\n"
                        + "BEGIN\n"
                        + "    procedure_step := 'insert log';\n"
                        + "    IF p_id > 0 THEN\n"
                        + "        output_code := '00';\n"
                        + "    END IF;\n"
                        + "    p_code := output_code;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("procedure_step VARCHAR2(500);")
                .contains("output_code VARCHAR2(10);")
                .contains("END IF")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesFunctionWithDeclarationsAndOptionalSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.message_count(\n"
                        + "    p_id IN INTEGER\n"
                        + ") RETURN INTEGER IS\n"
                        + "    result_count INTEGER;\n"
                        + "BEGIN\n"
                        + "    SELECT COUNT(*) INTO result_count\n"
                        + "    FROM app.logs WHERE id = p_id;\n"
                        + "    RETURN result_count;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("result_count INTEGER;")
                .contains("RETURN result_count")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void preservesPolarDbOracleReturnsIsDeclareFunctionSyntax() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION "
                        + "app.get_flyway_test_count()\n"
                        + "RETURNS number IS\n"
                        + "DECLARE\n"
                        + "    v_num number;\n"
                        + "BEGIN\n"
                        + "    SELECT 1 INTO v_num;\n"
                        + "    RETURN v_num;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("RETURNS number IS\nDECLARE")
                .contains("SELECT 1 INTO v_num;")
                .contains("RETURN v_num")
                .doesNotContain("SELECT 1;\n");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void doesNotTreatFunctionColumnAsNestedRoutine() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.latest_report_date(\n"
                        + "    p_function IN VARCHAR2\n"
                        + ") RETURN DATE IS\n"
                        + "    result_date DATE;\n"
                        + "BEGIN\n"
                        + "    /* SELECT function FROM old_reports; */\n"
                        + "    SELECT MAX(statement_date)\n"
                        + "    INTO result_date\n"
                        + "    FROM app.report_details\n"
                        + "    WHERE function = p_function;\n"
                        + "    RETURN result_date;\n"
                        + "EXCEPTION\n"
                        + "    WHEN OTHERS THEN\n"
                        + "        RETURN NULL;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("WHERE function = p_function")
                .contains("WHEN OTHERS THEN")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void doesNotTreatLoopLabelAsControlFlowBlock() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.process_values IS\n"
                        + "BEGIN\n"
                        + "    FOR value_index IN 1..10 LOOP\n"
                        + "        IF value_index = 5 THEN\n"
                        + "            GOTO loop;\n"
                        + "        END IF;\n"
                        + "        <<loop>>\n"
                        + "        NULL;\n"
                        + "    END LOOP;\n"
                        + "END process_values;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("GOTO loop")
                .contains("<<loop>>")
                .contains("END LOOP")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesFunctionWithCursorSqlCaseInDeclarationSection() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.get_policy_value(\n"
                        + "    p_policy_id IN INTEGER\n"
                        + ") RETURN VARCHAR2 IS\n"
                        + "    CURSOR policy_cursor IS\n"
                        + "        SELECT\n"
                        + "            CASE WHEN status = 'A'\n"
                        + "                 THEN CASE WHEN premium > 0\n"
                        + "                           THEN 'ACTIVE_PAID'\n"
                        + "                           ELSE 'ACTIVE' END\n"
                        + "                 ELSE 'OTHER' END "
                        + "AS status_text,\n"
                        + "            CASE WHEN premium = 0\n"
                        + "                 THEN '0.00' ELSE TO_CHAR(premium) "
                        + "END AS premium_text\n"
                        + "        FROM app.policies\n"
                        + "        WHERE policy_id = p_policy_id;\n"
                        + "    policy_row policy_cursor%ROWTYPE;\n"
                        + "BEGIN\n"
                        + "    OPEN policy_cursor;\n"
                        + "    FETCH policy_cursor INTO policy_row;\n"
                        + "    CLOSE policy_cursor;\n"
                        + "    RETURN policy_row.status_text;\n"
                        + "END get_policy_value;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("CASE WHEN status = 'A'")
                .contains("END AS premium_text")
                .contains("policy_row policy_cursor%ROWTYPE;")
                .contains("END get_policy_value")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesProcedureWithCursorSqlCaseInDeclarationSection() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.load_policy IS\n"
                        + "    CURSOR policy_cursor IS\n"
                        + "        SELECT CASE WHEN status = 'A'\n"
                        + "                    THEN 1 ELSE 0 END AS active\n"
                        + "        FROM app.policies;\n"
                        + "    active_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    SELECT 1 INTO active_value FROM dual;\n"
                        + "END load_policy;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("CURSOR policy_cursor IS")
                .contains("END AS active")
                .contains("active_value INTEGER;")
                .contains("END load_policy")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesRoutineWithCaseExpressionInitializer() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.classify_value(\n"
                        + "    p_value IN INTEGER\n"
                        + ") RETURN VARCHAR2 IS\n"
                        + "    value_class VARCHAR2(10) :=\n"
                        + "        CASE WHEN p_value > 0 "
                        + "THEN 'POSITIVE' ELSE 'OTHER' END;\n"
                        + "    declaration_after_case INTEGER := 1;\n"
                        + "BEGIN\n"
                        + "    RETURN value_class;\n"
                        + "END classify_value;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("CASE WHEN p_value > 0")
                .contains("declaration_after_case INTEGER := 1;")
                .contains("END classify_value")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesNestedRoutineWithCursorSqlCase() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.outer_proc IS\n"
                        + "    PROCEDURE inner_proc IS\n"
                        + "        CURSOR value_cursor IS\n"
                        + "            SELECT CASE WHEN value > 0\n"
                        + "                        THEN value ELSE 0 END "
                        + "AS normalized_value\n"
                        + "            FROM app.values;\n"
                        + "        value_row value_cursor%ROWTYPE;\n"
                        + "    BEGIN\n"
                        + "        NULL;\n"
                        + "    END inner_proc;\n"
                        + "    outer_value INTEGER := 1;\n"
                        + "BEGIN\n"
                        + "    inner_proc;\n"
                        + "END outer_proc;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE inner_proc")
                .contains("END AS normalized_value")
                .contains("value_row value_cursor%ROWTYPE;")
                .contains("outer_value INTEGER := 1;")
                .contains("END outer_proc")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesWithFunctionContainingCursorSqlCase() {
        List<SqlStatement> statements = parse(
                "WITH\n"
                        + "FUNCTION normalize_value(p_value INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "    CURSOR value_cursor IS\n"
                        + "        SELECT CASE WHEN p_value > 0\n"
                        + "                    THEN p_value ELSE 0 END "
                        + "AS normalized_value\n"
                        + "        FROM dual;\n"
                        + "    result_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    RETURN p_value;\n"
                        + "END;\n"
                        + "SELECT normalize_value(1) FROM dual;\n"
                        + "SELECT 2;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION normalize_value")
                .contains("END AS normalized_value")
                .contains("result_value INTEGER;")
                .contains("SELECT normalize_value(1) FROM dual")
                .doesNotContain("SELECT 2");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 2");
    }

    @Test
    void parsesProcedureWithNestedRoutineDeclaration() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.outer_proc IS\n"
                        + "    PROCEDURE inner_proc IS\n"
                        + "        inner_value INTEGER;\n"
                        + "    BEGIN\n"
                        + "        inner_value := 1;\n"
                        + "    END;\n"
                        + "BEGIN\n"
                        + "    inner_proc;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("inner_value INTEGER;")
                .contains("inner_proc;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesProcedureWithExceptionHandler() {
        List<SqlStatement> statements = parse(
                "/***************************************\n"
                        + " * Migration procedure.\n"
                        + " ***************************************/\n"
                        + "CREATE OR REPLACE PROCEDURE "
                        + "app.insert_search_result_log(\n"
                        + "    i_search_date IN DATE,\n"
                        + "    i_user_id IN VARCHAR2,\n"
                        + "    o_return_code OUT VARCHAR2,\n"
                        + "    o_return_message OUT VARCHAR2\n"
                        + ") IS\n"
                        + "    procedure_step VARCHAR2(500);\n"
                        + "    output_code VARCHAR2(10);\n"
                        + "BEGIN\n"
                        + "    DBMS_OUTPUT.PUT_LINE('Flyway test');\n"
                        + "    procedure_step := 'insert log';\n"
                        + "    INSERT INTO app.search_result_log\n"
                        + "        (search_date, user_id)\n"
                        + "    VALUES (i_search_date, i_user_id);\n"
                        + "    o_return_code := '00';\n"
                        + "EXCEPTION\n"
                        + "    WHEN OTHERS THEN\n"
                        + "        o_return_code := '99';\n"
                        + "        o_return_message := SQLERRM;\n"
                        + "        procedure_step := SQLCODE || ':' "
                        + "|| procedure_step;\n"
                        + "        app.insert_message(\n"
                        + "            SUBSTR(procedure_step, 1, 400),\n"
                        + "            output_code);\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("procedure_step VARCHAR2(500);")
                .contains("EXCEPTION")
                .contains("WHEN OTHERS THEN")
                .contains("output_code);")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesNamedNonEditionableProcedureWithSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE NONEDITIONABLE PROCEDURE "
                        + "\"App\".\"MixedCaseProc\" IS\n"
                        + "    message_text VARCHAR2(200) "
                        + ":= q'[value;with;semicolons]';\n"
                        + "BEGIN\n"
                        + "    NULL;\n"
                        + "END \"MixedCaseProc\";\n"
                        + "   /   \n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("NONEDITIONABLE PROCEDURE")
                .contains("q'[value;with;semicolons]'")
                .contains("END \"MixedCaseProc\"")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesNamedFunctionWithoutSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.calculate_total(\n"
                        + "    p_value IN INTEGER\n"
                        + ") RETURN INTEGER AUTHID DEFINER IS\n"
                        + "    calculated_value INTEGER := 0;\n"
                        + "BEGIN\n"
                        + "    calculated_value := p_value * 2;\n"
                        + "    RETURN calculated_value;\n"
                        + "END calculate_total;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("calculated_value INTEGER := 0;")
                .contains("END calculate_total")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesNestedLocalFunctionAndControlFlow() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.control_flow_proc IS\n"
                        + "    FUNCTION local_value(p_id INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "        result_value INTEGER;\n"
                        + "    BEGIN\n"
                        + "        result_value := p_id;\n"
                        + "        RETURN result_value;\n"
                        + "    END local_value;\n"
                        + "    current_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    current_value := local_value(1);\n"
                        + "    LOOP\n"
                        + "        CASE current_value\n"
                        + "            WHEN 1 THEN current_value := 2;\n"
                        + "            ELSE current_value := 0;\n"
                        + "        END CASE;\n"
                        + "        EXIT;\n"
                        + "    END LOOP;\n"
                        + "END control_flow_proc;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION local_value")
                .contains("END local_value")
                .contains("END CASE")
                .contains("END LOOP")
                .contains("END control_flow_proc")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesRoutineWithNestedBeginAndSqlCaseInBody() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.nested_block_proc IS\n"
                        + "    result_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    BEGIN\n"
                        + "        SELECT CASE WHEN value > 0\n"
                        + "                    THEN value ELSE 0 END\n"
                        + "        INTO result_value\n"
                        + "        FROM app.values;\n"
                        + "    END;\n"
                        + "    result_value := result_value + 1;\n"
                        + "END nested_block_proc;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("SELECT CASE WHEN value > 0")
                .contains("result_value := result_value + 1;")
                .contains("END nested_block_proc")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesBackToBackRoutinesWithoutSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.first_proc IS\n"
                        + "    first_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    first_value := 1;\n"
                        + "END;\n"
                        + "CREATE OR REPLACE FUNCTION app.second_func\n"
                        + "RETURN INTEGER IS\n"
                        + "    second_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    second_value := 2;\n"
                        + "    RETURN second_value;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(3);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE app.first_proc")
                .doesNotContain("FUNCTION app.second_func");
        assertThat(statements.get(1).getSql())
                .contains("FUNCTION app.second_func")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(2).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesTriggerWithDeclarationsAndNestedRoutines() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE TRIGGER app.audit_policy\n"
                        + "AFTER INSERT OR UPDATE OR DELETE "
                        + "ON app.policies\n"
                        + "REFERENCING NEW AS NEW OLD AS OLD\n"
                        + "FOR EACH ROW\n"
                        + "DECLARE\n"
                        + "    table_name CONSTANT VARCHAR2(30) "
                        + ":= 'POLICIES';\n"
                        + "    primary_key VARCHAR2(30) := :NEW.id;\n"
                        + "    PROCEDURE write_log(\n"
                        + "        column_name IN VARCHAR2,\n"
                        + "        old_value IN VARCHAR2,\n"
                        + "        new_value IN VARCHAR2\n"
                        + "    ) IS\n"
                        + "        transaction_id INTEGER;\n"
                        + "    BEGIN\n"
                        + "        transaction_id := 1;\n"
                        + "        INSERT INTO app.audit_log\n"
                        + "            (table_name, column_name, old_value, "
                        + "new_value)\n"
                        + "        VALUES\n"
                        + "            (table_name, column_name, old_value, "
                        + "new_value);\n"
                        + "    END write_log;\n"
                        + "BEGIN\n"
                        + "    IF UPDATING THEN\n"
                        + "        write_log('STATUS', :OLD.status, "
                        + ":NEW.status);\n"
                        + "    END IF;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("primary_key VARCHAR2(30) := :NEW.id;")
                .contains("PROCEDURE write_log")
                .contains("transaction_id INTEGER;")
                .contains("END write_log")
                .contains("END IF")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesTriggerWithCursorSqlCaseInDeclarationSection() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE TRIGGER app.audit_policy\n"
                        + "BEFORE UPDATE ON app.policies\n"
                        + "FOR EACH ROW\n"
                        + "DECLARE\n"
                        + "    CURSOR status_cursor IS\n"
                        + "        SELECT CASE WHEN :NEW.status = 'A'\n"
                        + "                    THEN 'ACTIVE' ELSE 'OTHER' END "
                        + "AS status_text\n"
                        + "        FROM dual;\n"
                        + "    status_row status_cursor%ROWTYPE;\n"
                        + "BEGIN\n"
                        + "    :NEW.updated_at := SYSDATE;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("CURSOR status_cursor IS")
                .contains("END AS status_text")
                .contains("status_row status_cursor%ROWTYPE;")
                .contains(":NEW.updated_at := SYSDATE;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesTriggerWithoutDeclarationSectionWithSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE TRIGGER app.set_created_at\n"
                        + "BEFORE INSERT ON app.messages\n"
                        + "FOR EACH ROW\n"
                        + "BEGIN\n"
                        + "    :NEW.created_at := SYSDATE;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains(":NEW.created_at := SYSDATE;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesCallStyleTriggerWithoutPlsqlBlock() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE TRIGGER app.notify_change\n"
                        + "AFTER UPDATE ON app.messages\n"
                        + "CALL app.notify_message();\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("CALL app.notify_message()")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesCompoundTriggerWithDeclarations() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE TRIGGER app.audit_messages\n"
                        + "FOR INSERT OR UPDATE ON app.messages\n"
                        + "COMPOUND TRIGGER\n"
                        + "    changed_rows INTEGER := 0;\n"
                        + "    PROCEDURE count_change IS\n"
                        + "        local_count INTEGER;\n"
                        + "    BEGIN\n"
                        + "        changed_rows := changed_rows + 1;\n"
                        + "    END count_change;\n"
                        + "    BEFORE EACH ROW IS\n"
                        + "    BEGIN\n"
                        + "        count_change;\n"
                        + "    END BEFORE EACH ROW;\n"
                        + "    AFTER STATEMENT IS\n"
                        + "    BEGIN\n"
                        + "        INSERT INTO app.audit_summary "
                        + "VALUES (changed_rows);\n"
                        + "    END AFTER STATEMENT;\n"
                        + "END audit_messages;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("COMPOUND TRIGGER")
                .contains("changed_rows INTEGER := 0;")
                .contains("PROCEDURE count_change")
                .contains("END BEFORE EACH ROW")
                .contains("END AFTER STATEMENT")
                .contains("END audit_messages")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesPackageSpecificationAndBody() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PACKAGE app.log_pkg AS\n"
                        + "    PROCEDURE add_log(p_id INTEGER);\n"
                        + "    FUNCTION log_count RETURN INTEGER;\n"
                        + "END log_pkg;\n"
                        + "/\n"
                        + "CREATE OR REPLACE PACKAGE BODY app.log_pkg AS\n"
                        + "    PROCEDURE add_log(p_id INTEGER) IS\n"
                        + "    BEGIN\n"
                        + "        INSERT INTO app.logs(id) VALUES (p_id);\n"
                        + "    END add_log;\n"
                        + "\n"
                        + "    FUNCTION log_count RETURN INTEGER IS\n"
                        + "        result INTEGER;\n"
                        + "    BEGIN\n"
                        + "        SELECT COUNT(*) INTO result FROM app.logs;\n"
                        + "        RETURN result;\n"
                        + "    END log_count;\n"
                        + "BEGIN\n"
                        + "    NULL;\n"
                        + "END log_pkg;\n"
                        + "/\n"
                        + "CALL app.log_pkg.add_log(1);\n");

        assertThat(statements)
                .extracting(SqlStatement::getSql)
                .hasSize(3);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION log_count")
                .doesNotContain("PACKAGE BODY");
        assertThat(statements.get(1).getSql())
                .contains("PACKAGE BODY")
                .contains("END add_log")
                .contains("END log_count")
                .contains("END log_pkg");
        assertThat(statements.get(2).getSql())
                .startsWith("CALL app.log_pkg.add_log");
    }

    @Test
    void parsesPackageSpecificationAndBodyWithoutSlash() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PACKAGE app.log_pkg AS\n"
                        + "    PROCEDURE add_log(p_id INTEGER);\n"
                        + "END log_pkg;\n"
                        + "CREATE OR REPLACE PACKAGE BODY app.log_pkg AS\n"
                        + "    PROCEDURE add_log(p_id INTEGER) IS\n"
                        + "    BEGIN\n"
                        + "        INSERT INTO app.logs(id) VALUES (p_id);\n"
                        + "    END add_log;\n"
                        + "BEGIN\n"
                        + "    NULL;\n"
                        + "END log_pkg;\n"
                        + "CALL app.log_pkg.add_log(1);\n");

        assertThat(statements)
                .extracting(SqlStatement::getSql)
                .hasSize(3);
        assertThat(statements.get(0).getSql())
                .contains("PACKAGE app.log_pkg")
                .doesNotContain("PACKAGE BODY");
        assertThat(statements.get(1).getSql())
                .contains("PACKAGE BODY")
                .contains("END add_log")
                .contains("END log_pkg")
                .doesNotContain("CALL app.log_pkg");
        assertThat(statements.get(2).getSql())
                .startsWith("CALL app.log_pkg.add_log");
    }

    @Test
    void parsesPackageBodyWithCommentsBeforeIs() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE EDITIONABLE PACKAGE BODY "
                        + "app.integration_pkg\n"
                        + "--------------------------------------------------\n"
                        + "-- Change history intentionally precedes IS.\n"
                        + "-- Revision: migration parser regression test.\n"
                        + "--------------------------------------------------\n"
                        + "IS\n"
                        + "    FUNCTION endpoint RETURN VARCHAR2 IS\n"
                        + "    BEGIN\n"
                        + "        RETURN '/health';\n"
                        + "    END endpoint;\n"
                        + "END integration_pkg;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PACKAGE BODY app.integration_pkg")
                .contains("Change history intentionally precedes IS")
                .contains("END integration_pkg")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesPackageBodyRoutineWithCursorSqlCase() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PACKAGE BODY app.policy_pkg AS\n"
                        + "    FUNCTION policy_status(p_id INTEGER) "
                        + "RETURN VARCHAR2 IS\n"
                        + "        CURSOR status_cursor IS\n"
                        + "            SELECT CASE WHEN status = 'A'\n"
                        + "                        THEN 'ACTIVE' ELSE 'OTHER' "
                        + "END AS status_text\n"
                        + "            FROM app.policies\n"
                        + "            WHERE id = p_id;\n"
                        + "        status_row status_cursor%ROWTYPE;\n"
                        + "    BEGIN\n"
                        + "        RETURN 'ACTIVE';\n"
                        + "    END policy_status;\n"
                        + "END policy_pkg;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PACKAGE BODY app.policy_pkg")
                .contains("END AS status_text")
                .contains("status_row status_cursor%ROWTYPE;")
                .contains("END policy_status")
                .contains("END policy_pkg")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void doesNotTreatEndTableAliasAsPackageTerminator() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PACKAGE BODY app.endpoint_pkg IS\n"
                        + "    FUNCTION endpoint_url(p_id INTEGER) "
                        + "RETURN VARCHAR2 IS\n"
                        + "        CURSOR endpoint_cursor IS\n"
                        + "            SELECT end.endpoint_url\n"
                        + "            FROM app.service_endpoint end\n"
                        + "            WHERE end.endpoint_id = p_id;\n"
                        + "        result_url VARCHAR2(200);\n"
                        + "    BEGIN\n"
                        + "        OPEN endpoint_cursor;\n"
                        + "        FETCH endpoint_cursor INTO result_url;\n"
                        + "        CLOSE endpoint_cursor;\n"
                        + "        RETURN result_url;\n"
                        + "    END endpoint_url;\n"
                        + "END endpoint_pkg;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("app.service_endpoint end")
                .contains("WHERE end.endpoint_id = p_id")
                .contains("END endpoint_pkg")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void doesNotTreatEndTableAliasAsRoutineTerminator() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.find_endpoint("
                        + "p_id INTEGER) RETURN VARCHAR2 IS\n"
                        + "    CURSOR endpoint_cursor IS\n"
                        + "        SELECT end.endpoint_url\n"
                        + "        FROM app.service_endpoint end\n"
                        + "        WHERE end.endpoint_id = p_id;\n"
                        + "    result_url VARCHAR2(200);\n"
                        + "BEGIN\n"
                        + "    RETURN result_url;\n"
                        + "END find_endpoint;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("app.service_endpoint end")
                .contains("END find_endpoint")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void doesNotTreatEndTableAliasAsAnonymousBlockTerminator() {
        List<SqlStatement> statements = parse(
                "BEGIN\n"
                        + "    INSERT INTO app.endpoint_log(endpoint_id)\n"
                        + "    SELECT end.endpoint_id\n"
                        + "    FROM app.service_endpoint end;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("SELECT end.endpoint_id")
                .contains("app.service_endpoint end")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesOracleConditionalCompilationInquiryDirectives() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PACKAGE BODY app.diagnostics_pkg IS\n"
                        + "    PROCEDURE log_error IS\n"
                        + "    BEGIN\n"
                        + "        dbms_output.put_line(\n"
                        + "            $$plsql_unit || ':' || "
                        + "$$plsql_line);\n"
                        + "    END log_error;\n"
                        + "END diagnostics_pkg;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("$$plsql_unit")
                .contains("$$plsql_line")
                .contains("END diagnostics_pkg")
                .doesNotContain("SELECT 1;");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlock() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    v_count INTEGER := 0;\n"
                        + "BEGIN\n"
                        + "    IF v_count = 0 THEN\n"
                        + "        v_count := v_count + 1;\n"
                        + "    END IF;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("v_count := v_count + 1")
                .contains("END IF");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlockWithCursorSqlCaseDeclaration() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    CURSOR value_cursor IS\n"
                        + "        SELECT CASE WHEN value > 0\n"
                        + "                    THEN value ELSE 0 END "
                        + "AS normalized_value\n"
                        + "        FROM app.values;\n"
                        + "    value_row value_cursor%ROWTYPE;\n"
                        + "BEGIN\n"
                        + "    OPEN value_cursor;\n"
                        + "    CLOSE value_cursor;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("CURSOR value_cursor IS")
                .contains("END AS normalized_value")
                .contains("value_row value_cursor%ROWTYPE;")
                .contains("CLOSE value_cursor;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlockWithLocalProcedure() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    assignment_count INTEGER := 0;\n"
                        + "    PROCEDURE proc_cmpn_assign(\n"
                        + "        p_delta IN INTEGER\n"
                        + "    ) IS\n"
                        + "    BEGIN\n"
                        + "        assignment_count := "
                        + "assignment_count + p_delta;\n"
                        + "    END proc_cmpn_assign;\n"
                        + "BEGIN\n"
                        + "    proc_cmpn_assign(1);\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE proc_cmpn_assign")
                .contains("END proc_cmpn_assign")
                .contains("proc_cmpn_assign(1);")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlockWithLocalFunction() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    calculated_value INTEGER;\n"
                        + "    FUNCTION local_value(p_value INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "    BEGIN\n"
                        + "        RETURN p_value + 1;\n"
                        + "    END local_value;\n"
                        + "BEGIN\n"
                        + "    calculated_value := local_value(1);\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION local_value")
                .contains("END local_value")
                .contains("calculated_value := local_value(1);")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlockWithSiblingLocalRoutinesWithoutSlash() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    current_value INTEGER := 0;\n"
                        + "    PROCEDURE set_value(p_value INTEGER) IS\n"
                        + "    BEGIN\n"
                        + "        current_value := p_value;\n"
                        + "    END;\n"
                        + "    FUNCTION increment_value(p_value INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "    BEGIN\n"
                        + "        RETURN p_value + 1;\n"
                        + "    END increment_value;\n"
                        + "    PROCEDURE apply_increment IS\n"
                        + "    BEGIN\n"
                        + "        current_value := "
                        + "increment_value(current_value);\n"
                        + "    END apply_increment;\n"
                        + "BEGIN\n"
                        + "    set_value(1);\n"
                        + "    apply_increment;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE set_value")
                .contains("FUNCTION increment_value")
                .contains("PROCEDURE apply_increment")
                .contains("apply_increment;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlockWithDeepMixedLocalRoutineNesting() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    final_value INTEGER := 0;\n"
                        + "    PROCEDURE level_one IS\n"
                        + "        FUNCTION level_two RETURN INTEGER IS\n"
                        + "            nested_value INTEGER := 0;\n"
                        + "            PROCEDURE level_three("
                        + "p_value OUT INTEGER) IS\n"
                        + "            BEGIN\n"
                        + "                p_value := 3;\n"
                        + "            END level_three;\n"
                        + "        BEGIN\n"
                        + "            level_three(nested_value);\n"
                        + "            RETURN nested_value;\n"
                        + "        END level_two;\n"
                        + "    BEGIN\n"
                        + "        final_value := level_two;\n"
                        + "    END level_one;\n"
                        + "BEGIN\n"
                        + "    level_one;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE level_one")
                .contains("FUNCTION level_two")
                .contains("PROCEDURE level_three")
                .contains("END level_three")
                .contains("END level_two")
                .contains("END level_one")
                .contains("level_one;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousLocalRoutineWithNestedBlockAndExceptionHandler() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    processed_count INTEGER := 0;\n"
                        + "    PROCEDURE process_values IS\n"
                        + "    BEGIN\n"
                        + "        FOR value_index IN 1..3 LOOP\n"
                        + "            DECLARE\n"
                        + "                normalized_value INTEGER;\n"
                        + "            BEGIN\n"
                        + "                CASE value_index\n"
                        + "                    WHEN 1 THEN "
                        + "normalized_value := 10;\n"
                        + "                    ELSE normalized_value := 0;\n"
                        + "                END CASE;\n"
                        + "                processed_count := "
                        + "processed_count + normalized_value;\n"
                        + "            EXCEPTION\n"
                        + "                WHEN OTHERS THEN NULL;\n"
                        + "            END;\n"
                        + "        END LOOP;\n"
                        + "    EXCEPTION\n"
                        + "        WHEN OTHERS THEN processed_count := -1;\n"
                        + "    END process_values;\n"
                        + "BEGIN\n"
                        + "    process_values;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE process_values")
                .contains("DECLARE\n                normalized_value")
                .contains("END CASE")
                .contains("END LOOP")
                .contains("END process_values")
                .contains("process_values;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesAnonymousBlockWithForwardDeclaredMutualRecursion() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    result_value BOOLEAN;\n"
                        + "    FUNCTION is_even(p_value INTEGER) "
                        + "RETURN BOOLEAN;\n"
                        + "    FUNCTION is_odd(p_value INTEGER) "
                        + "RETURN BOOLEAN IS\n"
                        + "    BEGIN\n"
                        + "        IF p_value = 0 THEN\n"
                        + "            RETURN FALSE;\n"
                        + "        END IF;\n"
                        + "        RETURN is_even(p_value - 1);\n"
                        + "    END is_odd;\n"
                        + "    FUNCTION is_even(p_value INTEGER) "
                        + "RETURN BOOLEAN IS\n"
                        + "    BEGIN\n"
                        + "        IF p_value = 0 THEN\n"
                        + "            RETURN TRUE;\n"
                        + "        END IF;\n"
                        + "        RETURN is_odd(p_value - 1);\n"
                        + "    END is_even;\n"
                        + "BEGIN\n"
                        + "    result_value := is_even(4);\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION is_even(p_value INTEGER) "
                        + "RETURN BOOLEAN;")
                .contains("END is_odd")
                .contains("END is_even")
                .contains("result_value := is_even(4);")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void doesNotTreatRoutineKeywordsInAnonymousSqlAsDeclarations() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    CURSOR routine_cursor IS\n"
                        + "        SELECT function, procedure\n"
                        + "        FROM app.routine_metadata\n"
                        + "        WHERE function = 'FUNCTION'\n"
                        + "          AND procedure = 'PROCEDURE';\n"
                        + "    routine_row routine_cursor%ROWTYPE;\n"
                        + "    PROCEDURE consume_row IS\n"
                        + "    BEGIN\n"
                        + "        NULL;\n"
                        + "    END consume_row;\n"
                        + "BEGIN\n"
                        + "    OPEN routine_cursor;\n"
                        + "    FETCH routine_cursor INTO routine_row;\n"
                        + "    CLOSE routine_cursor;\n"
                        + "    consume_row;\n"
                        + "END;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("SELECT function, procedure")
                .contains("WHERE function = 'FUNCTION'")
                .contains("AND procedure = 'PROCEDURE'")
                .contains("END consume_row")
                .contains("consume_row;")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void ignoresRoutineSyntaxInAnonymousBlockCommentsAndStrings() {
        List<SqlStatement> statements = parse(
                "DECLARE\n"
                        + "    message_text VARCHAR2(200) := "
                        + "'FUNCTION fake RETURN NUMBER;';\n"
                        + "    -- PROCEDURE disabled_proc IS BEGIN NULL; END;\n"
                        + "    /* FUNCTION disabled_func RETURN NUMBER IS\n"
                        + "       BEGIN RETURN 0; END disabled_func; */\n"
                        + "    PROCEDURE active_proc IS\n"
                        + "    BEGIN\n"
                        + "        message_text := "
                        + "'PROCEDURE text_only IS BEGIN NULL; END;';\n"
                        + "    END active_proc;\n"
                        + "BEGIN\n"
                        + "    active_proc;\n"
                        + "END;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION fake RETURN NUMBER;")
                .contains("PROCEDURE disabled_proc")
                .contains("FUNCTION disabled_func")
                .contains("PROCEDURE active_proc")
                .contains("END active_proc")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesOracleWithFunction() {
        List<SqlStatement> statements = parse(
                "with\n"
                        + "function inc_num(p_value INTEGER) "
                        + "return INTEGER is\n"
                        + "begin\n"
                        + "    return p_value + 1;\n"
                        + "end;\n"
                        + "select inc_num(1) from dual;\n"
                        + "SELECT 2;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .startsWith("with")
                .contains("return p_value + 1;")
                .contains("select inc_num(1) from dual")
                .doesNotContain("SELECT 2");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 2");
    }

    @Test
    void parsesMultipleOracleWithFunctionsWithOptionalSlash() {
        List<SqlStatement> statements = parse(
                "WITH\n"
                        + "FUNCTION lookup_value(p_id INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "    result_value INTEGER;\n"
                        + "BEGIN\n"
                        + "    SELECT p_id INTO result_value FROM dual;\n"
                        + "    RETURN result_value;\n"
                        + "END;\n"
                        + "FUNCTION double_value(p_id INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "BEGIN\n"
                        + "    IF p_id IS NULL THEN\n"
                        + "        RETURN 0;\n"
                        + "    END IF;\n"
                        + "    RETURN p_id * 2;\n"
                        + "END;\n"
                        + "SELECT double_value(lookup_value(2)) FROM dual\n"
                        + "/\n"
                        + "SELECT 3;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("FUNCTION lookup_value")
                .contains("SELECT p_id INTO result_value FROM dual;")
                .contains("FUNCTION double_value")
                .contains("END IF;")
                .contains("SELECT double_value(lookup_value(2)) FROM dual")
                .doesNotContain("SELECT 3");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 3");
    }

    @Test
    void parsesOracleWithProcedure() {
        List<SqlStatement> statements = parse(
                "WITH\n"
                        + "PROCEDURE write_log(p_value INTEGER) IS\n"
                        + "BEGIN\n"
                        + "    INSERT INTO app.logs(id) VALUES (p_value);\n"
                        + "END;\n"
                        + "SELECT 1 FROM dual;\n"
                        + "SELECT 2;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .startsWith("WITH")
                .contains("PROCEDURE write_log")
                .contains("INSERT INTO app.logs(id) VALUES (p_value);")
                .contains("SELECT 1 FROM dual")
                .doesNotContain("SELECT 2");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 2");
    }

    @Test
    void parsesMixedOracleWithProcedureAndFunctionWithSlash() {
        List<SqlStatement> statements = parse(
                "WITH\n"
                        + "PROCEDURE record_value(p_value INTEGER) IS\n"
                        + "BEGIN\n"
                        + "    NULL;\n"
                        + "END;\n"
                        + "FUNCTION double_value(p_value INTEGER) "
                        + "RETURN INTEGER IS\n"
                        + "BEGIN\n"
                        + "    record_value(p_value);\n"
                        + "    RETURN p_value * 2;\n"
                        + "END;\n"
                        + "SELECT double_value(2) FROM dual\n"
                        + "/\n"
                        + "SELECT 3;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PROCEDURE record_value")
                .contains("FUNCTION double_value")
                .contains("record_value(p_value);")
                .contains("SELECT double_value(2) FROM dual")
                .doesNotContain("SELECT 3");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 3");
    }

    @Test
    void preservesPostgreSqlCteNamedFunctionInPgMode() {
        List<SqlStatement> statements = parse(
                "WITH function AS (SELECT 1 AS value)\n"
                        + "SELECT value FROM function;\n"
                        + "SELECT 2;\n",
                true);

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("WITH function AS")
                .contains("SELECT value FROM function")
                .doesNotContain("SELECT 2");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 2");
    }

    @Test
    void preservesPostgreSqlDollarQuotedBlocks() {
        List<SqlStatement> statements = parse(
                "DO $block$\n"
                        + "BEGIN\n"
                        + "    PERFORM 'value;with;semicolons';\n"
                        + "END;\n"
                        + "$block$;\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PERFORM 'value;with;semicolons'");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void preservesPostgreSqlEmptyDollarQuotedBlocksInPgMode() {
        List<SqlStatement> statements = parse(
                "DO $$\n"
                        + "BEGIN\n"
                        + "    PERFORM 'value;with;semicolons';\n"
                        + "END;\n"
                        + "$$;\n"
                        + "SELECT 1;\n",
                true);

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PERFORM 'value;with;semicolons'");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void preservesPostgreSqlDollarQuotedFunctionInPgMode() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE FUNCTION app.message_count(\n"
                        + "    p_id INTEGER\n"
                        + ") RETURNS INTEGER\n"
                        + "LANGUAGE plpgsql\n"
                        + "AS $function$\n"
                        + "DECLARE\n"
                        + "    result_count INTEGER;\n"
                        + "BEGIN\n"
                        + "    SELECT COUNT(*) INTO result_count\n"
                        + "    FROM app.logs WHERE id = p_id;\n"
                        + "    RETURN result_count;\n"
                        + "END;\n"
                        + "$function$;\n"
                        + "SELECT 1;\n",
                true);

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("LANGUAGE plpgsql")
                .contains("$function$")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void preservesPostgreSqlDollarQuotedProcedureInPgMode() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PROCEDURE app.refresh_log(\n"
                        + "    p_id INTEGER\n"
                        + ")\n"
                        + "LANGUAGE plpgsql\n"
                        + "AS $procedure$\n"
                        + "BEGIN\n"
                        + "    INSERT INTO app.logs(id) VALUES (p_id);\n"
                        + "END;\n"
                        + "$procedure$;\n"
                        + "CALL app.refresh_log(1);\n",
                true);

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("LANGUAGE plpgsql")
                .contains("$procedure$")
                .doesNotContain("CALL app.refresh_log");
        assertThat(statements.get(1).getSql())
                .isEqualTo("CALL app.refresh_log(1)");
    }

    @Test
    void preservesPostgreSqlTriggerInPgMode() {
        List<SqlStatement> statements = parse(
                "CREATE TRIGGER audit_messages\n"
                        + "AFTER INSERT OR UPDATE ON app.messages\n"
                        + "FOR EACH ROW\n"
                        + "EXECUTE FUNCTION app.audit_message();\n"
                        + "SELECT 1;\n",
                true);

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("EXECUTE FUNCTION app.audit_message()")
                .doesNotContain("SELECT 1");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void parsesLegacyOracleCommentsWithDoubleDashDecorators() {
        List<SqlStatement> statements = parse(
                "CREATE OR REPLACE PACKAGE BODY app.comment_pkg AS\n"
                        + "/*\n"
                        + "--/***************************************\n"
                        + "--/* Disabled legacy procedure\n"
                        + "--/***************************************\n"
                        + "PROCEDURE disabled_proc IS\n"
                        + "BEGIN\n"
                        + "    NULL;\n"
                        + "END disabled_proc;\n"
                        + "*/\n"
                        + "PROCEDURE active_proc IS\n"
                        + "BEGIN\n"
                        + "    NULL;\n"
                        + "END active_proc;\n"
                        + "END comment_pkg;\n"
                        + "/\n"
                        + "SELECT 1;\n");

        assertThat(statements).hasSize(2);
        assertThat(statements.get(0).getSql())
                .contains("PACKAGE BODY app.comment_pkg")
                .contains("PROCEDURE active_proc");
        assertThat(statements.get(1).getSql()).isEqualTo("SELECT 1");
    }

    @Test
    void usesNonNestedBlockCommentsInOracleMode() {
        List<SqlStatement> statements = parse(
                "SELECT /* outer /* inner */ 1;\n");

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0).getSql())
                .contains("outer /* inner")
                .endsWith("1");
    }

    @Test
    void preservesPostgreSqlNestedBlockComments() {
        List<SqlStatement> statements = parse(
                "/* outer PostgreSQL comment\n"
                        + "   /* nested PostgreSQL comment */\n"
                        + "   back in the outer comment\n"
                        + "*/\n"
                        + "SELECT 1;\n",
                true);

        assertThat(statements).hasSize(1);
        assertThat(statements.get(0).getSql())
                .contains("nested PostgreSQL comment")
                .endsWith("SELECT 1");
    }

    private List<SqlStatement> parse(String sql) {
        return parse(sql, false);
    }

    private List<SqlStatement> parse(
            String sql,
            boolean postgreSqlMode) {
        PolarDBOracleParser parser = new PolarDBOracleParser(
                new FluentConfiguration(),
                new ParsingContext(),
                postgreSqlMode);
        List<SqlStatement> statements = new ArrayList<>();
        try (SqlStatementIterator iterator =
                parser.parse(new StringResource(sql))) {
            while (iterator.hasNext()) {
                statements.add(iterator.next());
            }
        }
        return statements;
    }
}
