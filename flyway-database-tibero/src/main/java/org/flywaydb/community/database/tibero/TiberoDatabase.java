package org.flywaydb.community.database.tibero;

import java.sql.Connection;
import org.flywaydb.core.api.configuration.Configuration;
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

    public static void enableTiberoTBDSNSupport() {
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
    public String getRawCreateScript(Table table, boolean b) {
        return "";
    }
}
