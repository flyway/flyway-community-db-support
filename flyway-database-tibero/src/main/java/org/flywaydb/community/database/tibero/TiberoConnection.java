package org.flywaydb.community.database.tibero;

import java.sql.SQLException;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

public class TiberoConnection extends Connection<TiberoDatabase> {

    protected TiberoConnection(TiberoDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL");
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        jdbcTemplate.execute("ALTER SESSION SET CURRENT_SCHEMA=" + database.quote(schema));
    }

    @Override
    public Schema doGetCurrentSchema() throws SQLException {
        String currentSchema = getCurrentSchemaNameOrSearchPath();

        return getSchema(currentSchema);
    }

    @Override
    public Schema getSchema(String name) {
        return new TiberoSchema(jdbcTemplate, database, name);
    }
}
