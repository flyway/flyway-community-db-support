package org.flywaydb.community.database.crate;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.Types;

public class CrateDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {
    @Override
    public String getName() {
        return "CrateDB";
    }

    @Override
    public int getNullType() {
        return Types.NULL;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return "org.postgresql.Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        return databaseProductName.contains("PostgreSQL");
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new CrateDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new CrateParser(configuration, parsingContext);
    }

    @Override
    public int getPriority() {
        // have a precedence over standard PostgreSQL
        return 1;
    }
}
