package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.internal.database.base.Connection;

import java.sql.SQLException;

public class DuckDBConnection extends Connection<DuckDBDatabase> {

    protected DuckDBConnection(DuckDBDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT current_schema()");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        final var sql = "USE %s;".formatted(schema);
        jdbcTemplate.execute(sql);
    }

    @Override
    public DuckDBSchema getSchema(String name) {
        return new DuckDBSchema(jdbcTemplate, database, name);
    }
}
