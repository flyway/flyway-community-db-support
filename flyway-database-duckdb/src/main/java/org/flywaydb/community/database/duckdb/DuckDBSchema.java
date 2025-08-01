/*-
 * ========================LICENSE_START=================================
 * flyway-database-duckdb
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
package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

public class DuckDBSchema extends Schema<DuckDBDatabase, DuckDBTable> {

    public DuckDBSchema(JdbcTemplate jdbcTemplate, DuckDBDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        final var countSchemasWithName = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?";
        return jdbcTemplate.queryForInt(countSchemasWithName, name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        final var countTablesInSchema = "SELECT count(*) from information_schema.tables WHERE table_schema = ?";
        return jdbcTemplate.queryForInt(countTablesInSchema, name) == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name));
    }

    @Override
    protected void doClean() throws SQLException {
        dropAll("MACRO", getAllMacros());
        dropAll("SEQUENCE", getAllObjectsNames("sequence_name", "duckdb_sequences()"));
        dropAll("VIEW", getAllViews());

        var tables = getAllTables();
        while (!tables.isEmpty()) {
            final var tablesWithoutIncomingRefs = tables.stream()
                .filter(table -> table.incomingRefsCount == 0)
                .map(table -> table.tableName)
                .toList();
            if (tablesWithoutIncomingRefs.isEmpty()) {
                throw new IllegalStateException("""
                    Cannot drop all tables in schema %s.
                    Duckdb does not support DROP TABLE if the table has incoming references.
                    All existing tables have such references.
                    """.formatted(name)
                );
            }
            dropAll("TABLE", tablesWithoutIncomingRefs);
            tables = getAllTables();
        }
    }

    @Override
    protected DuckDBTable[] doAllTables() throws SQLException {
        return getAllTables().stream()
            .map(table -> table.tableName)
            .map(this::getTable)
            .toList()
            .toArray(new DuckDBTable[]{});
    }

    @Override
    public DuckDBTable getTable(String tableName) {
        return new DuckDBTable(jdbcTemplate, database, this, tableName);
    }

    private void dropAll(String objectType, List<String> objectsNames) throws SQLException {
        for (final var objectName : objectsNames) {
            jdbcTemplate.execute("DROP %s %s.%s CASCADE".formatted(objectType, database.quote(name), database.quote(objectName)));
        }
    }

    private List<String> getAllMacros() throws SQLException {
        final var sql = "SELECT function_name FROM duckdb_functions() WHERE NOT internal AND schema_name = ?";
        return jdbcTemplate.queryForStringList(sql, name);
    }

    private List<String> getAllViews() throws SQLException {
        final var sql = "SELECT view_name FROM duckdb_views() WHERE NOT internal AND schema_name = ?";
        return jdbcTemplate.queryForStringList(sql, name);
    }

    private List<Table> getAllTables() throws SQLException {
        final var sql = """
            SELECT
                table_name,
                (
                    SELECT count(*)
                    FROM information_schema.referential_constraints rc
                    JOIN information_schema.key_column_usage to_column_usage
                        ON rc.unique_constraint_name = to_column_usage.constraint_name
                        AND rc.unique_constraint_schema = to_column_usage.constraint_schema
                    JOIN information_schema.key_column_usage from_column_usage
                        ON rc.constraint_name = from_column_usage.constraint_name
                        AND rc.constraint_schema = from_column_usage.constraint_schema
                    WHERE to_column_usage.constraint_schema = ?
                        AND to_column_usage.table_name = tbls.table_name
                        AND from_column_usage.table_name != tbls.table_name
                ) AS incoming_refs_count
            FROM duckdb_tables() tbls
            WHERE tbls.schema_name = ?;
        """;
        return jdbcTemplate.query(
            sql,
            rs -> new Table(rs.getString("table_name"), rs.getInt("incoming_refs_count")),
            name,
            name
        );
    }

    private record Table(String tableName, int incomingRefsCount) {
    }

    private List<String> getAllObjectsNames(String catalogNameField, String catalogTable) throws SQLException {
        final var sql = "SELECT %s FROM %s WHERE schema_name = ?".formatted(catalogNameField, catalogTable);
        return jdbcTemplate.queryForStringList(sql, name);
    }
}
