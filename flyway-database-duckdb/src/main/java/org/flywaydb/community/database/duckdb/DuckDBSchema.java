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
        dropAll("TABLE", getAllTablesNames());
    }

    @Override
    protected DuckDBTable[] doAllTables() throws SQLException {
        return getAllTablesNames().stream()
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

    private List<String> getAllTablesNames() throws SQLException {
        return getAllObjectsNames("table_name", "duckdb_tables()");
    }

    private List<String> getAllObjectsNames(String catalogNameField, String catalogTable) throws SQLException {
        final var sql = "SELECT %s FROM %s WHERE schema_name = ?".formatted(catalogNameField, catalogTable);
        return jdbcTemplate.queryForStringList(sql, name);
    }
}
