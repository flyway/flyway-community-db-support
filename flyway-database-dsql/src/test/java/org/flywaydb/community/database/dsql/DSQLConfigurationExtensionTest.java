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

import org.flywaydb.core.extensibility.ConfigurationExtension;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DSQLConfigurationExtensionTest {

    private final DSQLConfigurationExtension ext = new DSQLConfigurationExtension();

    @Test
    void namespaceIsDsql() {
        assertThat(ext.getNamespace()).isEqualTo("dsql");
    }

    @Test
    void occRetryIsOffByDefault() {
        assertThat(ext.getOccMaxRetries()).isZero();
        assertThat(ext.getOccMaxRetryDelaySeconds()).isEqualTo(5);
    }

    @Test
    void awaitAsyncIndexesDefaultsToFalse() {
        assertThat(ext.isAwaitAsyncIndexes()).isFalse();
    }

    @Test
    void settersRoundTrip() {
        ext.setOccMaxRetries(2);
        ext.setOccMaxRetryDelaySeconds(10);
        ext.setAwaitAsyncIndexes(true);
        assertThat(ext.getOccMaxRetries()).isEqualTo(2);
        assertThat(ext.getOccMaxRetryDelaySeconds()).isEqualTo(10);
        assertThat(ext.isAwaitAsyncIndexes()).isTrue();
    }

    @Test
    void envVarsMapToFlywayPrefixedKeys() {
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_OCC_MAX_RETRIES"))
                .isEqualTo("flyway.dsql.occMaxRetries");
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_OCC_MAX_RETRY_DELAY_SECONDS"))
                .isEqualTo("flyway.dsql.occMaxRetryDelaySeconds");
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_AWAIT_ASYNC_INDEXES"))
                .isEqualTo("flyway.dsql.awaitAsyncIndexes");
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_UNKNOWN")).isNull();
    }

    // flyway-core (ConfigUtils) returns this value verbatim as the resolved property key, so it
    // must be fully qualified with the "flyway." prefix — matching every core extension and the
    // ClickHouse sibling ("flyway.<namespace>.<property>"). Without the prefix the env override
    // never resolves (the bug this guards against).
    @Test
    void envKeysAreFullyQualifiedWithFlywayAndNamespace() {
        String prefix = "flyway." + ext.getNamespace() + ".";
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_OCC_MAX_RETRIES"))
                .startsWith(prefix);
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_OCC_MAX_RETRY_DELAY_SECONDS"))
                .startsWith(prefix);
        assertThat(ext.getConfigurationParameterFromEnvironmentVariable("FLYWAY_DSQL_AWAIT_ASYNC_INDEXES"))
                .startsWith(prefix);
    }

    @Test
    void copyPreservesValuesAndDoesNotThrow() {
        ext.setOccMaxRetries(4);
        ext.setOccMaxRetryDelaySeconds(15);
        ext.setAwaitAsyncIndexes(true);
        ConfigurationExtension copy = (ConfigurationExtension) ext.copy();
        assertThat(copy).isInstanceOf(DSQLConfigurationExtension.class);
        DSQLConfigurationExtension c = (DSQLConfigurationExtension) copy;
        assertThat(c.getOccMaxRetries()).isEqualTo(4);
        assertThat(c.getOccMaxRetryDelaySeconds()).isEqualTo(15);
        assertThat(c.isAwaitAsyncIndexes()).isTrue();
    }
}
