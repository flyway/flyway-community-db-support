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
        return "";
    }

    @Override
    public Schema getSchema(String s) {
        return null;
    }
}
