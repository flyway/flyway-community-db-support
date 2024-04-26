package org.flywaydb.community.database.crate;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

import java.sql.Connection;

public class CrateDatabase extends Database<CrateConnection> {

    public CrateDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected CrateConnection doGetConnection(Connection connection) {
        return new CrateConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
        // NOOP
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "TRUE";
    }

    @Override
    public String getBooleanFalse() {
        return "FALSE";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        return "CREATE TABLE " + table + """
                (
                    installed_rank INT NOT NULL PRIMARY KEY,
                    version VARCHAR(50),
                    description VARCHAR(200) NOT NULL,
                    type VARCHAR(20) NOT NULL,
                    script VARCHAR(1000) NOT NULL,
                    checksum INTEGER,
                    installed_by varchar(100) NOT NULL,
                    installed_on TIMESTAMP NOT NULL DEFAULT now(),
                    execution_time INTEGER NOT NULL,
                    success BOOLEAN NOT NULL
                );
                """ + (baseline ? getBaselineStatement(table) + ";\n" : "");
    }
}
