package org.flywaydb.community.database.tibero;

import java.sql.SQLException;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

public class TiberoTable extends Table<TiberoDatabase, TiberoSchema> {

    public TiberoTable(JdbcTemplate jdbcTemplate, TiberoDatabase database, TiberoSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return false;
    }

    @Override
    protected void doLock() throws SQLException {

    }

    @Override
    protected void doDrop() throws SQLException {

    }
}
