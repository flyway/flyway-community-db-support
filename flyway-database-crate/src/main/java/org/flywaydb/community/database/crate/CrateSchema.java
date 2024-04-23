package org.flywaydb.community.database.crate;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;

public class CrateSchema extends Schema<CrateDatabase, CrateTable> {
    public CrateSchema(JdbcTemplate jdbcTemplate, CrateDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT() FROM information_schema.schemata WHERE schema_name = ?",
                name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return jdbcTemplate.queryForInt("""
                        SELECT COUNT() FROM (
                        SELECT table_name FROM information_schema.tables WHERE table_schema = ? UNION
                        SELECT table_name FROM information_schema.views WHERE table_schema = ? UNION
                        SELECT rountine_name FROM information_schema.routines WHERE routine_schema = ?
                        ) objs""",
                name, name, name) == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        // schema cannot be explicitly created
        // NOOP
    }

    @Override
    protected void doDrop() throws SQLException {
        // schema cannot be explicitly removed
        // NOOP
    }

    @Override
    protected void doClean() throws SQLException {

    }

    @Override
    protected CrateTable[] doAllTables() throws SQLException {
        return jdbcTemplate
                .queryForStringList("SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                        name)
                .stream()
                .map( tableName -> new CrateTable(jdbcTemplate, database, this, tableName) )
                .toArray(CrateTable[]::new);
    }

    @Override
    public Table getTable(String tableName) {
        return new CrateTable(jdbcTemplate, database, this, tableName);
    }

}
