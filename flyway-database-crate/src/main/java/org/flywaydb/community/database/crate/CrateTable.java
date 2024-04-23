package org.flywaydb.community.database.crate;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class CrateTable extends Table<CrateDatabase, CrateSchema> {
    public CrateTable(JdbcTemplate jdbcTemplate, CrateDatabase database, CrateSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TABLE " + this);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt(
                "SELECT COUNT() FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                schema.getName(), name) > 0;
    }

    @Override
    protected void doLock() throws SQLException {
        // NOOP - There is no support for locking in CrateDB
    }
}
