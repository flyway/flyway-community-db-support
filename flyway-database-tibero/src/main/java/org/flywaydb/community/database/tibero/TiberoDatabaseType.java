package org.flywaydb.community.database.tibero;

import java.sql.Connection;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class TiberoDatabaseType extends BaseDatabaseType implements CommunityDatabaseType {

    @Override
    public String getName() {
        return "";
    }

    @Override
    public int getNullType() {
        return 0;
    }

    @Override
    public boolean handlesJDBCUrl(String s) {
        return false;
    }

    @Override
    public String getDriverClass(String s, ClassLoader classLoader) {
        return "";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String s, String s1, Connection connection) {
        return false;
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory,
        StatementInterceptor statementInterceptor) {
        return null;
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider,
        ParsingContext parsingContext) {
        return null;
    }
}
