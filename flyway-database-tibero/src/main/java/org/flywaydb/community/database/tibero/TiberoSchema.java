package org.flywaydb.community.database.tibero;


import java.sql.SQLException;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

public class TiberoSchema extends Schema<TiberoDatabase, TiberoTable> {

    public TiberoSchema(JdbcTemplate jdbcTemplate, TiberoDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return false;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return false;
    }

    @Override
    protected void doCreate() throws SQLException {

    }

    @Override
    protected void doDrop() throws SQLException {

    }

    @Override
    protected void doClean() throws SQLException {

    }

    @Override
    protected TiberoTable[] doAllTables() throws SQLException {
        return new TiberoTable[0];
    }

    @Override
    public Table getTable(String s) {
        return null;
    }
}
