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

/**
 * Configuration for Aurora DSQL behavior, under the {@code dsql} namespace.
 *
 * <p>OCC retry is off by default ({@code occMaxRetries=0}). Enable it via
 * {@code flyway.dsql.occMaxRetries} (a positive value) and tune the backoff cap with
 * {@code flyway.dsql.occMaxRetryDelaySeconds} (or the matching {@code FLYWAY_DSQL_*}
 * environment variables); 3 retries with a 5s cap is a reasonable starting point.
 *
 * <p>Async index waiting is opt-in, off by default. Set
 * {@code flyway.dsql.awaitAsyncIndexes=true} (or {@code FLYWAY_DSQL_AWAIT_ASYNC_INDEXES}) to
 * block on {@code CREATE INDEX ASYNC} builds until they complete.
 *
 * <p>Must remain a pure JavaBean: Flyway deep-copies it via {@link #copy()} using Jackson.
 * Do not store computed / non-bean state here.
 */
public class DSQLConfigurationExtension implements ConfigurationExtension {

    private static final String ENV_MAX_RETRIES = "FLYWAY_DSQL_OCC_MAX_RETRIES";
    private static final String ENV_MAX_RETRY_DELAY_SECONDS = "FLYWAY_DSQL_OCC_MAX_RETRY_DELAY_SECONDS";
    private static final String ENV_AWAIT_ASYNC_INDEXES = "FLYWAY_DSQL_AWAIT_ASYNC_INDEXES";

    private int occMaxRetries = 0;
    private int occMaxRetryDelaySeconds = 5;
    private boolean awaitAsyncIndexes = false;

    public int getOccMaxRetries() {
        return occMaxRetries;
    }

    public void setOccMaxRetries(int occMaxRetries) {
        this.occMaxRetries = occMaxRetries;
    }

    public int getOccMaxRetryDelaySeconds() {
        return occMaxRetryDelaySeconds;
    }

    public void setOccMaxRetryDelaySeconds(int occMaxRetryDelaySeconds) {
        this.occMaxRetryDelaySeconds = occMaxRetryDelaySeconds;
    }

    public boolean isAwaitAsyncIndexes() {
        return awaitAsyncIndexes;
    }

    public void setAwaitAsyncIndexes(boolean awaitAsyncIndexes) {
        this.awaitAsyncIndexes = awaitAsyncIndexes;
    }

    @Override
    public String getNamespace() {
        return "dsql";
    }

    @Override
    public String getConfigurationParameterFromEnvironmentVariable(String environmentVariable) {
        switch (environmentVariable) {
            case ENV_MAX_RETRIES:
                return "flyway.dsql.occMaxRetries";
            case ENV_MAX_RETRY_DELAY_SECONDS:
                return "flyway.dsql.occMaxRetryDelaySeconds";
            case ENV_AWAIT_ASYNC_INDEXES:
                return "flyway.dsql.awaitAsyncIndexes";
            default:
                return null;
        }
    }
}
