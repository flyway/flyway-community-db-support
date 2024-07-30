package org.flywaydb.community.database.tibero;

import java.sql.Connection;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;


public class TiberoDatabase extends Database<TiberoConnection> {

    public TiberoDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory,
        StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected TiberoConnection doGetConnection(Connection connection) {
        return null;
    }

    @Override
    public void ensureSupported(Configuration configuration) {

    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "";
    }

    @Override
    public String getBooleanFalse() {
        return "";
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
