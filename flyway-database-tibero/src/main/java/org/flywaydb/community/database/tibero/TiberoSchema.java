package org.flywaydb.community.database.tibero;


import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

public class TiberoSchema extends Schema<TiberoDatabase, TiberoTable> {

    public TiberoSchema(JdbcTemplate jdbcTemplate, TiberoDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    boolean isDefaultSchemaForUser() throws SQLException {
        return name.equals(database.doGetCurrentUser());
    }

    @Override
    protected boolean doExists() throws SQLException {
        return database.queryReturnsRows("SELECT * FROM ALL_USERS WHERE USERNAME = ?", name);
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return true;
    }

    @Override
    protected void doCreate() throws SQLException {
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP USER " + database.quote(name) + " CASCADE");
    }

    @Override
    protected void doClean() throws SQLException {

    }

    @Override
    protected TiberoTable[] doAllTables() throws SQLException {
        return new TiberoTable[1];
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Table getTable(String tableName) {
        return new TiberoTable(jdbcTemplate, database, this, tableName);
    }

    public enum ObjectType {
        TABLE("TABLE"),
        ;

        private final String name;
        private final String dropOptions;

        ObjectType(String name, String dropOptions) {
            this.name = name;
            this.dropOptions = dropOptions;
        }

        ObjectType(String name) {
            this(name, "");
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return super.toString().replace('_', ' ');
        }

        public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
            TiberoSchema schema) throws SQLException {
            return jdbcTemplate.queryForStringList(
                "SELECT DISTINCT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = ? AND OBJECT_TYPE = ?",
                schema.getName(), this.getName()
            );
        }

        public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
            TiberoSchema schema, String objectName) {
            return "DROP " + this.getName() + " " + database.quote(schema.getName(), objectName) +
                (StringUtils.hasText(dropOptions) ? " " + dropOptions : "");
        }

        public void dropObjects(JdbcTemplate jdbcTemplate, TiberoDatabase database,
            TiberoSchema schema) throws SQLException {
            for (String objectName : getObjectNames(jdbcTemplate, database, schema)) {
                jdbcTemplate.execute(
                    generateDropStatement(jdbcTemplate, database, schema, objectName));
            }
        }

        private void warnUnsupported(String schemaName, String typeDesc) {

        }

        private void warnUnsupported(String schemaName) {
            warnUnsupported(schemaName, this.toString().toLowerCase() + "s");
        }
    }
}
