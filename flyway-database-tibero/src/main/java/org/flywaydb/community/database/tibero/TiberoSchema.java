package org.flywaydb.community.database.tibero;


import static org.flywaydb.community.database.tibero.TiberoSchema.ObjectType.*;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            SCHEDULER_PROGRAM,
            SCHEDULE,
            SQL_TRANSLATION_PROFILE,
            MATERIALIZED_VIEW,
            MATERIALIZED_VIEW_LOG,
            DIMENSION,
            VIEW,
            TABLE,
            INDEX,
            SEQUENCE,
            FUNCTION,
            PROCEDURE,
            PACKAGE,
            PACKAGE_BODY,
            LIBRARY,
            TYPE,
            DIRECTORY,
            SYNONYM,

            // Object types with sensitive information (passwords), skip intentionally, print warning if found.
            DATABASE_LINK,
            CREDENTIAL
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

    private boolean locatorMetadataExists() throws SQLException {
        return database.queryReturnsRows("SELECT * FROM ALL_GEOMETRY_COLUMNS WHERE F_TABLE_SCHEMA = ?",
            name);
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

    private void disableFlashbackArchiveForFbaTrackedTables() throws SQLException {
        boolean dbaViewAccessible = database.isPrivOrRoleGranted("SELECT ANY DICTIONARY")
            || database.isDataDictViewAccessible("DBMS_FLASHBACK");

        if (!dbaViewAccessible && !isDefaultSchemaForUser()) {
            return;
        }

        String queryForRecycleBinStatus =
            "SELECT VALUE FROM " + database.quote("SYS", "V$PARAMETERS") + " WHERE NAME = 'USE_RECYCLEBIN'";
        String recycleBinStatus = jdbcTemplate.queryForString(queryForRecycleBinStatus);

        if (!"NO".equalsIgnoreCase(recycleBinStatus)) {
            jdbcTemplate.execute("ALTER SYSTEM SET USE_RECYCLEBIN = N");

            while (!"NO".equalsIgnoreCase(jdbcTemplate.queryForString(queryForRecycleBinStatus))) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new FlywayException("Interrupted while waiting for recycle bin to be disabled.", e);
                }
            }
        }
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

        SCHEDULE("SCHEDULER_SCHEDULES") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT SCHEDULE_NAME FROM " + database.dbaOrAll(SCHEDULE.getName()) + " WHERE OWNER = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "BEGIN DBMS_SCHEDULER.DROP_SCHEDULE('" + objectName + "'); END;";
            }
        },

        SQL_TRANSLATION_PROFILE("SQL TRANSLATION PROFILE") {
            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "BEGIN DBMS_SQL_TRANSLATOR.DROP_PROFILE('" + database.quote(schema.getName(),
                    objectName) + "'); END;";
            }
        },

        MATERIALIZED_VIEW("MATERIALIZED VIEW", "PRESERVE TABLE"),

        MATERIALIZED_VIEW_LOG("MATERIALIZED VIEW LOG") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT MASTER FROM ALL_MVIEW_LOGS WHERE LOG_OWNER = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "DROP " + this.getName() + " ON " + database.quote(schema.getName(),
                    objectName);
            }
        },

        DIMENSION("DIMENSION") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT 'DIMENSION' FROM DUAL",
                    schema.getName()
                );
            }
        },
        VIEW("VIEW"),
        TABLE("TABLE", "CASCADE CONSTRAINTS PURGE") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {

                StringBuilder tablesQuery = new StringBuilder();
                tablesQuery.append("WITH TABLES AS (\n" +
                    "  SELECT TABLE_NAME, OWNER\n" +
                    "  FROM ALL_TABLES\n" +
                    "  WHERE OWNER = ?\n" +
                    "    AND (IOT_TYPE IS NULL OR IOT_TYPE NOT LIKE '%OVERFLOW%')\n");

                tablesQuery.append(")\n" +
                    "SELECT TABLES.TABLE_NAME\n" +
                    "FROM TABLES\n" +
                    "WHERE NOT EXISTS (\n" +
                    "  SELECT 1\n" +
                    "  FROM ALL_QUEUE_TABLES\n" +
                    "  WHERE ALL_QUEUE_TABLES.OWNER = TABLES.OWNER\n" +
                    "    AND ALL_QUEUE_TABLES.QUEUE_TABLE = TABLES.TABLE_NAME\n" +
                    ")");

                String[] params = new String[1];
                Arrays.fill(params, schema.getName());

                return jdbcTemplate.queryForStringList(tablesQuery.toString(), params);
            }
        },
        INDEX("INDEX") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT INDEX_NAME FROM ALL_INDEXES WHERE OWNER = ?" +
                        " AND INDEX_NAME NOT LIKE 'SYS_C%'" +
                        " AND UNIQUENESS <> 'UNIQUE'",
                    schema.getName()
                );
            }
        },
        SEQUENCE("SEQUENCE"),
        FUNCTION("FUNCTION"),
        PROCEDURE("PROCEDURE"),
        PACKAGE("PACKAGE"),
        PACKAGE_BODY("PACKAGE BODY"),
        LIBRARY("LIBRARY"),
        TYPE("TYPE", "FORCE"),
        DIRECTORY("DIRECTORY"),
        SYNONYM("SYNONYM", "FORCE"),
        DATABASE_LINK("DATABASE LINK") {
            @Override
            public void dropObjects(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) {
                super.warnUnsupported(database.quote(schema.getName()));
            }

            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT DB_LINK FROM " + database.dbaOrAll("DB_LINKS") + " WHERE OWNER = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "DROP " + this.getName() + " "
                    + objectName;
            }
        },
        CREDENTIAL("CREDENTIAL") {
            @Override
            public void dropObjects(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) {
                super.warnUnsupported(database.quote(schema.getName()));
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "BEGIN DBMS_SCHEDULER.DROP_CREDENTIAL('" + database.quote(schema.getName(),
                    objectName) + "', FORCE => TRUE); END;";
            }
        },
        CONTEXT("CONTEXT") {
            @Override
            public List<String> getObjectNames(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema) throws SQLException {
                return jdbcTemplate.queryForStringList(
                    "SELECT NAMESPACE FROM " + database.dbaOrAll("CONTEXT") + " WHERE SCHEMA = ?",
                    schema.getName()
                );
            }

            @Override
            public String generateDropStatement(JdbcTemplate jdbcTemplate, TiberoDatabase database,
                TiberoSchema schema, String objectName) {
                return "DROP " + this.getName() + " " + database.quote(objectName); // no owner
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

        public static Set<String> getObjectTypeNames(JdbcTemplate jdbcTemplate,
            TiberoDatabase database, TiberoSchema schema) throws SQLException {
            String query =
                "SELECT DISTINCT OBJECT_TYPE FROM " + database.dbaOrAll("OBJECTS") + " WHERE OWNER = ? "
                    +
                    "UNION SELECT '" + MATERIALIZED_VIEW_LOG.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM ALL_MVIEW_LOGS WHERE LOG_OWNER = ?) "
                    +
                    "UNION SELECT '" + QUEUE_TABLE.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM ALL_QUEUE_TABLES WHERE OWNER = ?) "
                    +
                    "UNION SELECT '" + DATABASE_LINK.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM " + database.dbaOrAll("DB_LINKS") + " WHERE OWNER = ?) " +
                    "UNION SELECT '" + CONTEXT.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM V$CONTEXT WHERE NAMESPACE = ?) "
                    +
                    "UNION SELECT '" + CREDENTIAL.getName() + "' FROM DUAL "
                    +
                    "UNION SELECT '" + SCHEDULER_PROGRAM.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM " + database.dbaOrAll(SCHEDULER_PROGRAM.getName()) + " WHERE OWNER = ?) "
                    +
                    "UNION SELECT '" + SCHEDULE.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM " + database.dbaOrAll(SCHEDULE.getName()) + " WHERE OWNER = ?) "
                    +
                    "UNION SELECT '" + SCHEDULER_JOB.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM " + database.dbaOrAll(SCHEDULER_JOB.getName()) + " WHERE OWNER = ?) "
                    +
                    "UNION SELECT '" + SCHEDULER_CHAIN.getName() + "' FROM DUAL WHERE EXISTS(" +
                    "SELECT * FROM " + database.dbaOrAll(SCHEDULER_CHAIN.getName()) + " WHERE OWNER_ID IN ("
                    + "SELECT DISTINCT USER_ID FROM ALL_USERS WHERE USERNAME = ?)"
                    + ") ";

            String[] params = new String[9];
            Arrays.fill(params, schema.getName());

            return new HashSet<>(jdbcTemplate.queryForStringList(query, params));
        }
    }
}
