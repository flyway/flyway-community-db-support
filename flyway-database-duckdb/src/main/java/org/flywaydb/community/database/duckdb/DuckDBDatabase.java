package org.flywaydb.community.database.duckdb;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

public class DuckDBDatabase extends Database<DuckDBConnection> {

    public DuckDBDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected DuckDBConnection doGetConnection(java.sql.Connection connection) {
        return new DuckDBConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
    }

    @Override
    public boolean supportsDdlTransactions() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "true";
    }

    @Override
    public String getBooleanFalse() {
        return "false";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        final var createTable = """
            CREATE TABLE %s (
                installed_rank INTEGER NOT NULL,
                version        VARCHAR,
                description    VARCHAR NOT NULL,
                type           VARCHAR NOT NULL,
                script         VARCHAR NOT NULL,
                checksum       INTEGER,
                installed_by   VARCHAR NOT NULL,
                installed_on   TIMESTAMP NOT NULL DEFAULT now(),
                execution_time INTEGER NOT NULL,
                success        BOOLEAN NOT NULL
            );
        """.formatted(table);
        final var baselineStatement = baseline ? getBaselineStatement(table) + ";\n" : "";

        return createTable + baselineStatement;
    }
}
