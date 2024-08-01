package org.flywaydb.community.database.tibero;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.extensibility.Tier;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.StringUtils;

public class TiberoDatabase extends Database<TiberoConnection> {

    public TiberoDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory,
        StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    public static void enableTiberoTNSNameSupport() {
        String tiberoAdminEnvVar = System.getenv("TIBERO_ADMIN");
        String tiberoAdminSysProp = System.getProperty("TIBERO_NET_ADMIN");
        if (StringUtils.hasLength(tiberoAdminEnvVar) && tiberoAdminSysProp == null) {
            System.setProperty("TIBERO_NET_ADMIN", tiberoAdminEnvVar);
        }
    }

    @Override
    protected TiberoConnection doGetConnection(Connection connection) {
        return new TiberoConnection(this, connection);
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        return getMainConnection().getJdbcTemplate().queryForString("SELECT USER FROM DUAL");
    }

    @Override
    public void ensureSupported(Configuration configuration) {
        ensureDatabaseIsRecentEnough("7.0");
        ensureDatabaseNotOlderThanOtherwiseRecommendUpgradeToFlywayEdition("7.0", Tier.PREMIUM, configuration);
        recommendFlywayUpgradeIfNecessaryForMajorVersion("21.3");
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String tablespace = configuration.getTablespace() == null
            ? ""
            : " TABLESPACE \"" + configuration.getTablespace() + "\"";

        return "CREATE TABLE " + table + " (\n" +
            "    \"installed_rank\" NUMBER NOT NULL,\n" +
            "    \"version\" VARCHAR2(50),\n" +
            "    \"description\" VARCHAR2(200) NOT NULL,\n" +
            "    \"type\" VARCHAR2(20) NOT NULL,\n" +
            "    \"script\" VARCHAR2(1000) NOT NULL,\n" +
            "    \"checksum\" NUMBER,\n" +
            "    \"installed_by\" VARCHAR2(100) NOT NULL,\n" +
            "    \"installed_on\" TIMESTAMP DEFAULT SYSDATE NOT NULL,\n" +
            "    \"execution_time\" NUMBER NOT NULL,\n" +
            "    \"success\" NUMBER(1) NOT NULL,\n" +
            "    CONSTRAINT \"" + table.getName() + "_pk\" PRIMARY KEY (\"installed_rank\")\n" +
            ")" + tablespace + ";\n" +
            (baseline ? getBaselineStatement(table) + ";\n" : "") +
            "CREATE INDEX \"" + table.getSchema().getName() + "\".\"" + table.getName() + "_s_idx\" ON " + table
            + " (\"success\");\n";
    }

    boolean isXmlDbAvailable() throws SQLException {
        return isDataDictViewAccessible("ALL_XML_TABLES");
    }

    boolean isPrivOrRoleGranted(String name) throws SQLException {
        return queryReturnsRows("SELECT 1 FROM SESSION_PRIVS WHERE PRIVILEGE = ? UNION ALL " +
            "SELECT 1 FROM SESSION_ROLES WHERE ROLE = ?", name, name);
    }

    boolean queryReturnsRows(String query, String... params) throws SQLException {
        return getMainConnection().getJdbcTemplate()
            .queryForBoolean("SELECT CASE WHEN EXISTS(" + query + ") THEN 1 ELSE 0 END FROM DUAL", params);
    }

    private boolean isDataDictViewAccessible(String owner, String name) throws SQLException {
        return queryReturnsRows("SELECT * FROM DBA_TAB_PRIVS WHERE OWNER = ? AND TABLE_NAME = ?" +
            " AND PRIVILEGE = 'SELECT'", owner, name);
    }

    boolean isDataDictViewAccessible(String name) throws SQLException {
        return isDataDictViewAccessible("SYS", name);
    }

    Set<String> getSystemSchemas() throws SQLException {

        Set<String> result = new HashSet<>(Arrays.asList(
            "SYS", "SYSTEM",
            "SYSCAT",
            "SYSMAN",
            "SYSGIS",
            "OUTLN",
            "TIBERO",
            "TIBERO1"
        ));

        result.addAll(getMainConnection().getJdbcTemplate().queryForStringList(
            "SELECT USERNAME FROM DBA_USERS WHERE USERNAME LIKE 'SYS%'"
        ));

        return result;
    }

    boolean isFlashbackDataArchiveAvailable(String schemaName) throws SQLException {
        String paramQuery = "SELECT COUNT(*) FROM V$PARAMETERS WHERE NAME LIKE '%FLASHBACK%' AND VALUE IS NOT NULL AND (LENGTH(TRIM(VALUE)) > 0 OR VALUE != '0')";
        int paramCount = getMainConnection().getJdbcTemplate().queryForInt(paramQuery);

        if (paramCount == 0) {
            return false;
        }

        String destQuery = "SELECT VALUE FROM V$PARAMETERS WHERE NAME = 'FLASHBACK_LOG_ARCHIVE_DEST'";
        String flashbackDest = getMainConnection().getJdbcTemplate().queryForString(destQuery);

        if (flashbackDest == null || flashbackDest.trim().isEmpty()) {
            return false;
        }

        String logModeQuery = "SELECT LOG_MODE FROM V$DATABASE";
        String logMode = getMainConnection().getJdbcTemplate().queryForString(logModeQuery);

        if (!"ARCHIVELOG".equalsIgnoreCase(logMode)) {
            return false;
        }

        String tablespaceQuery = "SELECT COUNT(*) FROM DBA_USERS u JOIN V$TABLESPACE t ON u.DEFAULT_TABLESPACE = t.NAME WHERE u.USERNAME = ? AND t.FLASHBACK_ON = 'YES'";
        int flashbackOnCount = getMainConnection().getJdbcTemplate()
                .queryForInt(tablespaceQuery, schemaName.toUpperCase());

        if (flashbackOnCount == 0) {
            return false;
        }

        String privilegeQuery = "SELECT COUNT(*) FROM DBA_SYS_PRIVS WHERE GRANTEE = ? AND (PRIVILEGE = 'FLASHBACK ANY TABLE' OR PRIVILEGE = 'FLASHBACK OBJECT')";
        int privilegeCount = getMainConnection().getJdbcTemplate()
                .queryForInt(privilegeQuery, schemaName.toUpperCase());

        return privilegeCount != 0;
    }

    String dbaOrAll(String baseName) throws SQLException {
        return isPrivOrRoleGranted("SELECT ANY DICTIONARY") || isDataDictViewAccessible("DBA_" + baseName)
            ? "DBA_" + baseName
            : "ALL_" + baseName;
    }

    boolean isLocatorAvailable() throws SQLException {
        return isDataDictViewAccessible("SYSGIS", "ALL_GEOMETRY_COLUMNS");
    }
}
