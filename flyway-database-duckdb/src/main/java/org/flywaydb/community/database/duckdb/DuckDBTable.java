package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class DuckDBTable extends Table<DuckDBDatabase, DuckDBSchema> {

    public DuckDBTable(JdbcTemplate jdbcTemplate, DuckDBDatabase database, DuckDBSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForBoolean(
            "SELECT count(*) = 1 FROM duckdb_tables() WHERE schema_name = ? and table_name = ?",
            schema.getName(),
            name
        );
    }

    @Override
    protected void doLock() {
        // DuckDB does not support table level locks
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE %s.%s CASCADE;".formatted(database.quote(schema.getName()), database.quote(name)));
    }
}
