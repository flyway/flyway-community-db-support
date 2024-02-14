package org.flywaydb.community.database.example;

import java.sql.Connection;
import org.flywaydb.community.database.ExampleDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class ExampleDatabaseType extends BaseDatabaseType {

  @Override
  public String getName() {
    return null;
  }

  @Override
  public int getNullType() {
    return 0;
  }

  @Override
  public boolean handlesJDBCUrl(final String url) {
    return false;
  }

  @Override
  public String getDriverClass(final String url, final ClassLoader classLoader) {
    return null;
  }

  @Override
  public boolean handlesDatabaseProductNameAndVersion(final String databaseProductName,
      final String databaseProductVersion,
      final Connection connection) {
    return false;
  }

  @Override
  public Database createDatabase(final Configuration configuration, final JdbcConnectionFactory jdbcConnectionFactory,
      final StatementInterceptor statementInterceptor) {
    return null;
  }

  @Override
  public Parser createParser(final Configuration configuration, final ResourceProvider resourceProvider,
      final ParsingContext parsingContext) {
    return null;
  }

  @Override
  public String getPluginVersion(Configuration config) {
    return ExampleDatabaseExtension.readVersion();
  }
}