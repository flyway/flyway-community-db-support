package org.flywaydb.community.database.tibero;


import static org.flywaydb.community.database.tibero.TiberoSchema.ObjectType.QUEUE_TABLE;
import static org.flywaydb.community.database.tibero.TiberoSchema.ObjectType.SCHEDULER_CHAIN;
import static org.flywaydb.community.database.tibero.TiberoSchema.ObjectType.SCHEDULER_JOB;
import static org.flywaydb.community.database.tibero.TiberoSchema.ObjectType.SCHEDULER_PROGRAM;
import static org.flywaydb.community.database.tibero.TiberoSchema.ObjectType.TRIGGER;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.flywaydb.core.api.FlywayException;
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
        if (isSystem()) {
            throw new FlywayException("Clean not supported on Tibero for system schema " + database.quote(name) + "! " +
                "It must not be changed in any way except by running an Tibero-supplied script!");
        }

        if (database.isFlashbackDataArchiveAvailable(name)) {
            disableFlashbackArchiveForFbaTrackedTables();
        }

        if (database.isLocatorAvailable()) {
            cleanLocatorMetadata();
        }

        Set<String> objectTypeNames = ObjectType.getObjectTypeNames(jdbcTemplate, database, this);

        List<ObjectType> objectTypesToClean = List.of(
            TRIGGER,
            QUEUE_TABLE,
            SCHEDULER_CHAIN,
            SCHEDULER_JOB,
            SCHEDULER_PROGRAM
            );

        for (ObjectType objectType : objectTypesToClean) {
            if (objectTypeNames.contains(objectType.getName())) {
                objectType.dropObjects(jdbcTemplate, database, this);
            }
        }

        if (isDefaultSchemaForUser()) {
            jdbcTemplate.execute("PURGE RECYCLEBIN");
        }
    }

    public boolean isSystem() throws SQLException {
        return database.getSystemSchemas().contains(name);
    }

    private void cleanLocatorMetadata() throws SQLException {
        if (!locatorMetadataExists()) {
            return;
        }

        if (!isDefaultSchemaForUser()) {
            return;
        }

        jdbcTemplate.getConnection().commit();
        jdbcTemplate.execute("DELETE FROM USER_GEOMETRY_COLUMNS");
        jdbcTemplate.getConnection().commit();
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
        TRIGGER("TRIGGER"),
        QUEUE_TABLE("QUEUE TABLE") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT QUEUE_TABLE FROM ALL_QUEUE_TABLES WHERE OWNER = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "BEGIN DBMS_AQADM.DROP_QUEUE_TABLE('" + objectName + "'); END;";
            }
        },
        SCHEDULER_CHAIN("SCHEDULER_CHAINS") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT CHAIN_NAME FROM " + database.dbaOrAll(SCHEDULER_CHAIN.getName()) + " WHERE OWNER_ID IN ("
                        + "SELECT DISTINCT USER_ID FROM ALL_USERS WHERE USERNAME = ? "
                        + ") ", schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {

                deleteChainRules(jdbcTemplate, database, objectName);
                deleteChainSteps(jdbcTemplate, database, objectName);
                return "BEGIN DBMS_SCHEDULER.DROP_CHAIN('" + objectName + "'); END;";
            }

            private void deleteChainRules(JdbcTemplate jdbcTemplate, TiberoDatabase database, String objectName) {
                try {
                    final String SCHEDULER_JOB_RULE_TABLE_NAME = database.dbaOrAll("SCHEDULER_RULES");

                    List<String> ruleNames = jdbcTemplate.queryForStringList(
                        "SELECT RULE_NAME FROM " + SCHEDULER_JOB_RULE_TABLE_NAME + " WHERE CHAIN_NAME = ?",
                        objectName);

                    for (String ruleName : ruleNames) {
                        jdbcTemplate.execute(
                            "BEGIN DBMS_SCHEDULER.DROP_CHAIN_RULE('" + objectName + "','" + ruleName + "'); END;");
                    }

                } catch (SQLException e) {
                }
            }

            private void deleteChainSteps(JdbcTemplate jdbcTemplate, TiberoDatabase database, String objectName) {
                try {
                    final String SCHEDULER_JOB_STEP_TABLE_NAME = database.dbaOrAll("SCHEDULER_STEPS");

                    List<String> stepNames = jdbcTemplate.queryForStringList(
                        "SELECT STEP_NAME FROM " + SCHEDULER_JOB_STEP_TABLE_NAME + " WHERE CHAIN_NAME = ?",
                        objectName);

                    for (String stepName : stepNames) {
                        jdbcTemplate.execute(
                            "BEGIN DBMS_SCHEDULER.DROP_CHAIN_STEP('" + objectName + "','" + stepName + "'); END;");
                    }

                } catch (SQLException e) {
                }
            }
        },

        SCHEDULER_JOB("SCHEDULER_JOBS") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT JOB_NAME FROM " + database.dbaOrAll(SCHEDULER_JOB.getName()) + " WHERE OWNER = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {

                try {
                    final String SCHEDULER_JOB_LOG_TABLE_NAME = database.dbaOrAll("SCHEDULER_JOB_LOG");

                    jdbcTemplate.update("DELETE FROM " + SCHEDULER_JOB_LOG_TABLE_NAME + " WHERE JOB_NAME = ?",
                        objectName);
                } catch (SQLException e) {
                }

                return "BEGIN DBMS_SCHEDULER.DROP_JOB('" + objectName + "'); END;";
            }
        },

        SCHEDULER_PROGRAM("SCHEDULER_PROGRAMS") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT PROGRAM_NAME FROM " + database.dbaOrAll(SCHEDULER_PROGRAM.getName()) + " WHERE OWNER = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "BEGIN DBMS_SCHEDULER.DROP_PROGRAM('" + objectName + "'); END;";
            }
        },
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
