package org.flywaydb.community.database.crate;

import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

import java.sql.SQLException;

public class CrateConnection extends Connection<CrateDatabase> {
    public CrateConnection(CrateDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT current_schema");
    }

    @Override
    public Schema getSchema(String name) {
        return new CrateSchema(jdbcTemplate, database, name);
    }
}
