/*-
 * ========================LICENSE_START=================================
 * flyway-database-crate
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
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
package org.flywaydb.community.database.crate;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

public class CrateSchema extends Schema<CrateDatabase, CrateTable> {
    public CrateSchema(JdbcTemplate jdbcTemplate, CrateDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT() FROM information_schema.schemata WHERE schema_name = ?",
                name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return jdbcTemplate.queryForInt("""
                        SELECT COUNT() FROM (
                        SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema = ? UNION
                        SELECT table_name FROM information_schema.views WHERE table_schema = ? UNION
                        SELECT routine_name FROM information_schema.routines WHERE routine_schema = ?
                        ) objs""",
                name, name, name) == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        // schema cannot be explicitly created
        // NOOP
    }

    @Override
    protected void doDrop() throws SQLException {
        // schema cannot be explicitly removed
        // NOOP
    }

    @Override
    protected void doClean() throws SQLException {
        // drop views
        List<String> dropViewStatements = jdbcTemplate.queryForStringList(
                "SELECT table_name FROM information_schema.views WHERE table_schema = ?", name)
                .stream().map(viewName -> "DROP VIEW IF EXISTS "+database.quote(name, viewName)).toList();
        for (String stmt : dropViewStatements) {
            jdbcTemplate.execute(stmt);
        }

        // drop tables
        List<String> dropTableStatements = jdbcTemplate.queryForStringList(
                "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema = ?", name)
                .stream().map(tableName -> "DROP TABLE IF EXISTS "+database.quote(name, tableName)).toList();
        for (String stmt : dropTableStatements) {
            jdbcTemplate.execute(stmt);
        }

        // drop functions
        List<String> dropFunctionStatements = jdbcTemplate.queryForList(
                "SELECT routine_name, specific_name FROM information_schema.routines WHERE routine_schema = ?", name)
                .stream().map(result -> {
                    String fnName = result.get("routine_name");
                    String fnArgs = result.get("specific_name").substring(fnName.length());
                    return "DROP FUNCTION IF EXISTS " + database.quote(name, fnName) + fnArgs;
                }).toList();
        for (String stmt : dropFunctionStatements) {
            jdbcTemplate.execute(stmt);
        }
    }

    @Override
    protected CrateTable[] doAllTables() throws SQLException {
        return jdbcTemplate
                .queryForStringList("SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                        name)
                .stream()
                .map( tableName -> new CrateTable(jdbcTemplate, database, this, tableName) )
                .toArray(CrateTable[]::new);
    }

    @Override
    public Table getTable(String tableName) {
        return new CrateTable(jdbcTemplate, database, this, tableName);
    }

}
