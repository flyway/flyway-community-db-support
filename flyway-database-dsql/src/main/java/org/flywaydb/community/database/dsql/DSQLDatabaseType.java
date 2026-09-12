/*-
 * ========================LICENSE_START=================================
 * flyway-database-dsql
 * ========================================================================
 * Copyright (C) 2010 - 2026 Red Gate Software Ltd
 * ========================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.flywaydb.community.database.dsql;

import org.flywaydb.community.database.DSQLDatabaseExtension;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.callback.CallbackExecutor;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.ExecutionTemplate;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;
import org.flywaydb.database.postgresql.PostgreSQLDatabaseType;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Flyway database type for Amazon Aurora DSQL.
 *
 * <p>Extends PostgreSQL support to handle {@code jdbc:aws-dsql:postgresql://} URLs and DSQL endpoints.
 * The Aurora DSQL JDBC connector transforms {@code jdbc:aws-dsql:postgresql://} URLs to
 * {@code jdbc:postgresql://} internally, so DSQL is detected by both the URL prefix and the
 * endpoint host pattern ({@code .dsql.} for public endpoints, {@code .dsql-} for PrivateLink).
 */
public class DSQLDatabaseType extends PostgreSQLDatabaseType implements CommunityDatabaseType {

    // The only prefix the Aurora DSQL JDBC connector accepts (ConnUrlParser.isDsqlUrl); matching a
    // broader jdbc:aws-dsql: prefix would claim URLs the connector then refuses to open.
    private static final String DSQL_CONNECTOR_URL_PREFIX = "jdbc:aws-dsql:postgresql://";
    private static final String DSQL_PUBLIC_PATTERN = ".dsql.";
    private static final String DSQL_PRIVATELINK_PATTERN = ".dsql-";
    private static final int DEFAULT_MAX_RETRIES = 0;
    private static final int DEFAULT_MAX_RETRY_DELAY_SECONDS = 5;
    private static final long MIN_RETRY_DELAY_MS = 100L;

    private static final int[] DEFAULT_OCC_RETRY_KNOBS =
            {DEFAULT_MAX_RETRIES, DEFAULT_MAX_RETRY_DELAY_SECONDS};

    // DatabaseType is a JVM-global singleton resolved from a static registry, and the commit-wrapping
    // seam (createTransactionalExecutionTemplate) receives only a Connection, no Configuration. The
    // OCC retry knobs are therefore resolved in alterConnectionAsNeeded, which receives both, and
    // keyed on that Connection. Weak keys so entries go when Flyway closes the connection.
    // JdbcConnectionFactory skips alterConnectionAsNeeded for intercepted connections, which
    // therefore fall back to the defaults (retry off). That path captures SQL rather than committing
    // it, so there is no commit to lose an OCC race.
    private static final Map<Connection, int[]> OCC_RETRY_KNOBS =
            Collections.synchronizedMap(new WeakHashMap<>());

    @Override
    public String getName() {
        return "Aurora DSQL";
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        if (url.startsWith(DSQL_CONNECTOR_URL_PREFIX)) {
            return true;
        }
        return url.startsWith("jdbc:postgresql://") && isDsqlHost(url);
    }

    /**
     * Matches the DSQL endpoint pattern against the URL's host only, so a {@code .dsql.} appearing
     * elsewhere (database name, query parameter) cannot misclassify a plain PostgreSQL URL as DSQL.
     */
    private static boolean isDsqlHost(String jdbcUrl) {
        String host = extractHost(jdbcUrl);
        return host != null
                && (host.contains(DSQL_PUBLIC_PATTERN) || host.contains(DSQL_PRIVATELINK_PATTERN));
    }

    /** Extracts the host from a {@code jdbc:...} URL, or null if it cannot be parsed. */
    private static String extractHost(String jdbcUrl) {
        int scheme = jdbcUrl.indexOf("://");
        if (scheme < 0) {
            return null;
        }
        try {
            // Strip the "jdbc:" prefix so java.net.URI sees a single, parseable scheme.
            URI uri = new URI(jdbcUrl.substring(jdbcUrl.indexOf(':') + 1));
            return uri.getHost();
        } catch (URISyntaxException e) {
            return null;
        }
    }

    @Override
    public int getPriority() {
        // Higher than PostgreSQL (0) so DSQL URLs are matched first.
        return 1;
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName,
                                                        String databaseProductVersion,
                                                        Connection connection) {
        try {
            String url = connection.getMetaData().getURL();
            return url != null && isDsqlHost(url);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        if (url.startsWith(DSQL_CONNECTOR_URL_PREFIX)) {
            return "software.amazon.dsql.jdbc.DSQLConnector";
        }
        return "org.postgresql.Driver";
    }

    /**
     * Resolves the OCC retry knobs for this connection. Called for each connection Flyway opens
     * directly, and receives the {@link Configuration} that {@link #createTransactionalExecutionTemplate}
     * does not, so it is where the configured values enter the retry decorator.
     */
    @Override
    public Connection alterConnectionAsNeeded(Connection connection, Configuration configuration) {
        OCC_RETRY_KNOBS.put(connection, resolveOccRetryKnobs(configuration));
        return super.alterConnectionAsNeeded(connection, configuration);
    }

    @Override
    public Database createDatabase(Configuration configuration,
                                   JdbcConnectionFactory jdbcConnectionFactory,
                                   StatementInterceptor statementInterceptor) {
        return new DSQLDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    /**
     * Wraps the base transactional template so a commit-time OCC conflict retries the whole
     * migration transaction. This is the only Flyway seam that spans {@code connection.commit()},
     * where DSQL surfaces {@code OC000}/{@code OC001}/{@code 40001}.
     */
    @Override
    public ExecutionTemplate createTransactionalExecutionTemplate(Connection connection, boolean rollbackOnException) {
        ExecutionTemplate delegate = super.createTransactionalExecutionTemplate(connection, rollbackOnException);
        int[] knobs = OCC_RETRY_KNOBS.getOrDefault(connection, DEFAULT_OCC_RETRY_KNOBS);
        return new DSQLExecutionTemplate(delegate, knobs[0], maxRetryDelayMillis(knobs[1]));
    }

    /** Converts the configured delay cap (seconds) to milliseconds, floored at {@link #MIN_RETRY_DELAY_MS}. */
    static long maxRetryDelayMillis(int maxRetryDelaySeconds) {
        return Math.max(MIN_RETRY_DELAY_MS, (long) maxRetryDelaySeconds * 1000L);
    }

    static int[] resolveOccRetryKnobs(Configuration configuration) {
        if (configuration == null) {
            return DEFAULT_OCC_RETRY_KNOBS.clone();
        }
        DSQLConfigurationExtension ext =
                configuration.getPluginRegister().getPlugin(DSQLConfigurationExtension.class);
        if (ext == null) {
            return DEFAULT_OCC_RETRY_KNOBS.clone();
        }
        // The connector accepts a retry count of 0 to 100 and rejects anything outside that range;
        // clamp so a stray config value degrades gracefully instead of failing the migration.
        int maxRetries = Math.min(100, Math.max(0, ext.getOccMaxRetries()));
        return new int[]{maxRetries, ext.getOccMaxRetryDelaySeconds()};
    }

    /**
     * Wraps the base SQL-script executor factory so that, when {@code flyway.dsql.awaitAsyncIndexes}
     * is enabled, a migration's {@code CREATE INDEX ASYNC} blocks until the index build completes.
     * DSQL builds indexes in the background and returns a runtime {@code job_id}; static SQL cannot
     * thread that id into {@code sys.wait_for_job}, so the wait is issued here. Off by default. See
     * {@link DSQLSqlScriptExecutor}.
     */
    @Override
    public SqlScriptExecutorFactory createSqlScriptExecutorFactory(
            JdbcConnectionFactory jdbcConnectionFactory,
            CallbackExecutor callbackExecutor,
            StatementInterceptor statementInterceptor) {
        SqlScriptExecutorFactory delegate = super.createSqlScriptExecutorFactory(
                jdbcConnectionFactory, callbackExecutor, statementInterceptor);
        return new DSQLSqlScriptExecutorFactory(delegate);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider,
                               ParsingContext parsingContext) {
        return new DSQLParser(configuration, parsingContext);
    }

    @Override
    public String getPluginVersion(Configuration config) {
        return DSQLDatabaseExtension.readVersion();
    }
}
