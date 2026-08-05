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

import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.internal.database.base.CommunityDatabaseType;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.ExecutionTemplate;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DSQLDatabaseTypeTest {

    private final DSQLDatabaseType databaseType = new DSQLDatabaseType();

    @Test
    void nameIsAuroraDsql() {
        assertThat(databaseType.getName()).isEqualTo("Aurora DSQL");
    }

    @Test
    void isCommunityDatabaseType() {
        assertThat(databaseType).isInstanceOf(CommunityDatabaseType.class);
    }

    @Test
    void handlesAwsDsqlUrls() {
        assertThat(databaseType.handlesJDBCUrl("jdbc:aws-dsql:postgresql://abc123.dsql.us-east-1.on.aws/postgres")).isTrue();
        assertThat(databaseType.handlesJDBCUrl("jdbc:aws-dsql:postgresql://abc123.dsql.us-east-1.on.aws:5432/postgres")).isTrue();
    }

    @Test
    void doesNotHandleUrlShapesTheConnectorRejects() {
        // The connector accepts only jdbc:aws-dsql:postgresql:// (ConnUrlParser.isDsqlUrl), so
        // claiming a broader prefix would select this type for a URL no driver can open.
        assertThat(databaseType.handlesJDBCUrl("jdbc:aws-dsql://abc123.dsql.us-east-1.on.aws/postgres")).isFalse();
        assertThat(databaseType.handlesJDBCUrl("jdbc:aws-dsql:abc123.dsql.us-east-1.on.aws/postgres")).isFalse();
    }

    @Test
    void handlesTransformedPostgresqlUrlsWithDsqlEndpoint() {
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://abc123.dsql.us-east-1.on.aws:5432/postgres")).isTrue();
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://xyz789.dsql.eu-west-1.on.aws:5432/postgres")).isTrue();
    }

    @Test
    void handlesPrivateLinkEndpoints() {
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://abc123.dsql-fnh4.us-east-1.on.aws:5432/postgres")).isTrue();
        assertThat(databaseType.handlesJDBCUrl("jdbc:aws-dsql:postgresql://abc123.dsql-fnh4.us-east-1.on.aws:5432/postgres")).isTrue();
    }

    @Test
    void doesNotHandleNonDsqlUrls() {
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://localhost:5432/mydb")).isFalse();
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb")).isFalse();
        assertThat(databaseType.handlesJDBCUrl("jdbc:mysql://localhost:3306/mydb")).isFalse();
        assertThat(databaseType.handlesJDBCUrl("jdbc:oracle:thin:@localhost:1521:xe")).isFalse();
    }

    @Test
    void doesNotHandleDsqlPatternOutsideHost() {
        // The DSQL pattern must match the host only: a ".dsql." in the database name or a query
        // parameter must not hijack an ordinary PostgreSQL URL (priority 1 would win selection).
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://localhost:5432/app.dsql.production")).isFalse();
        assertThat(databaseType.handlesJDBCUrl("jdbc:postgresql://localhost:5432/app?ApplicationName=.dsql.")).isFalse();
    }

    @Test
    void priorityIsHigherThanPostgres() {
        assertThat(databaseType.getPriority()).isGreaterThan(0);
    }

    @Test
    void driverClassIsDsqlConnectorForAwsDsqlUrls() {
        assertThat(databaseType.getDriverClass("jdbc:aws-dsql:postgresql://abc123.dsql.us-east-1.on.aws/postgres", null))
                .isEqualTo("software.amazon.dsql.jdbc.DSQLConnector");
    }

    @Test
    void driverClassIsPostgresForTransformedUrls() {
        assertThat(databaseType.getDriverClass("jdbc:postgresql://abc123.dsql.us-east-1.on.aws:5432/postgres", null))
                .isEqualTo("org.postgresql.Driver");
    }

    @Test
    void wrapsTransactionalTemplateWithOccRetryDecorator() {
        // The commit-wrapping seam must return our OCC-retry decorator, otherwise a commit-time
        // DSQL conflict is never retried. Base PostgreSQL just news up a TransactionalExecutionTemplate
        // holding the connection (no I/O), so a null connection is fine for the type check.
        ExecutionTemplate template = databaseType.createTransactionalExecutionTemplate(null, true);
        assertThat(template).isInstanceOf(DSQLExecutionTemplate.class);
    }

    @Test
    void configuredRetryCountReachesTheDecorator() {
        // Joins the whole chain: configuration -> alterConnectionAsNeeded -> the transactional seam
        // -> decorator. Asserted through behaviour (attempt count), so dropping the plumbing fails
        // here instead of leaving the suite green.
        FluentConfiguration config = new FluentConfiguration();
        DSQLConfigurationExtension ext =
                config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class);
        ext.setOccMaxRetries(2);
        ext.setOccMaxRetryDelaySeconds(0); // floors at 100ms; keeps the backoff short

        Connection connection = occConflictingConnection();
        databaseType.alterConnectionAsNeeded(connection, config);
        ExecutionTemplate template = databaseType.createTransactionalExecutionTemplate(connection, true);

        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> template.execute(attempts::incrementAndGet))
                .isInstanceOf(FlywaySqlException.class);
        assertThat(attempts.get()).isEqualTo(3); // initial attempt + the 2 configured retries
    }

    @Test
    void unregisteredConnectionUsesTheDefaultRetryCount() {
        // Intercepted connections never reach alterConnectionAsNeeded, so they get the defaults;
        // OCC retry is off by default, so such a connection must not retry.
        ExecutionTemplate template =
                databaseType.createTransactionalExecutionTemplate(occConflictingConnection(), true);

        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> template.execute(attempts::incrementAndGet))
                .isInstanceOf(FlywaySqlException.class);
        assertThat(attempts.get()).isEqualTo(1);
    }

    /** Connection whose commit() reports a DSQL OCC conflict, where a real one also surfaces. */
    private static Connection occConflictingConnection() {
        return (Connection) Proxy.newProxyInstance(
                DSQLDatabaseTypeTest.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "commit":
                            throw new SQLException("write conflict", "OC000");
                        case "getAutoCommit":
                            return false;
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        case "toString":
                            return "occConflictingConnection";
                        default:
                            return method.getReturnType() == boolean.class ? Boolean.FALSE : null;
                    }
                });
    }

    @Test
    void wrapsSqlScriptExecutorFactoryForAsyncIndexWait() {
        // The SQL-script executor seam must return our wrapper so CREATE INDEX ASYNC waits.
        // Base PostgreSQL builds the delegate factory lazily (no I/O until an executor is made),
        // so null collaborators are fine for the type check.
        org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory factory =
                databaseType.createSqlScriptExecutorFactory(null, null, null);
        assertThat(factory).isInstanceOf(DSQLSqlScriptExecutorFactory.class);
    }

    @Test
    void maxRetryDelayConvertsSecondsToMillis() {
        // Guards the seconds->millis conversion: dropping the *1000 would make backoff 1000x too short.
        assertThat(DSQLDatabaseType.maxRetryDelayMillis(30)).isEqualTo(30_000L);
    }

    @Test
    void maxRetryDelayFloorsAtMinimum() {
        // A zero or negative configured cap must not disable backoff; it floors at 100ms.
        assertThat(DSQLDatabaseType.maxRetryDelayMillis(0)).isEqualTo(100L);
        assertThat(DSQLDatabaseType.maxRetryDelayMillis(-5)).isEqualTo(100L);
    }

    @Test
    void resolveKnobsFallsBackToDefaultsWhenConfigNull() {
        assertThat(DSQLDatabaseType.resolveOccRetryKnobs(null)).containsExactly(0, 5);
    }

    @Test
    void resolveKnobsReadsConfiguredExtensionValues() {
        // Drives the real resolution path: FluentConfiguration auto-registers the extension via
        // ServiceLoader. That the resolved values reach the decorator is covered separately by
        // configuredRetryCountReachesTheDecorator.
        FluentConfiguration config = new FluentConfiguration();
        DSQLConfigurationExtension ext =
                config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class);
        ext.setOccMaxRetries(3);
        ext.setOccMaxRetryDelaySeconds(10);
        assertThat(DSQLDatabaseType.resolveOccRetryKnobs(config)).containsExactly(3, 10);
    }

    @Test
    void resolveKnobsClampsNegativeRetriesToZero() {
        FluentConfiguration config = new FluentConfiguration();
        config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class).setOccMaxRetries(-1);
        assertThat(DSQLDatabaseType.resolveOccRetryKnobs(config)[0]).isZero();
    }

    @Test
    void resolveKnobsClampsExcessiveRetriesToConnectorMax() {
        // The clamp keeps an oversized config value from looping for hours.
        FluentConfiguration config = new FluentConfiguration();
        config.getPluginRegister().getPlugin(DSQLConfigurationExtension.class).setOccMaxRetries(500);
        assertThat(DSQLDatabaseType.resolveOccRetryKnobs(config)[0]).isEqualTo(100);
    }
}
