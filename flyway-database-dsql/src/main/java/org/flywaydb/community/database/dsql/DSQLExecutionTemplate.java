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

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.jdbc.ExecutionTemplate;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retries a whole migration transaction on an Aurora DSQL OCC conflict.
 *
 * <p>DSQL detects conflicts at commit time ({@code OC000}/{@code OC001}/{@code 40001}). Flyway
 * runs each migration inside an {@link ExecutionTemplate} whose {@code execute} calls
 * {@code connection.commit()} <em>after</em> the callback, so a conflict surfaces at the
 * transaction boundary — not inside a single statement. This decorator wraps the delegate
 * transactional template and retries the entire callback (statements + commit) when the
 * failure carries an OCC SQLSTATE. A conflicting transaction is rolled back before the retry, so
 * nothing was committed to replay; SQL migrations are re-parsed and re-run cleanly. (A Java
 * migration with external side effects would re-execute those side effects on each attempt.)
 *
 * <p>Replaying the callback also replays Flyway's in-memory bookkeeping: a migration that commits
 * only after a retry can appear more than once in the reported {@code MigrateResult}. The
 * migration's own statements roll back with the failed commit, but the schema-history insert runs
 * on the auto-commit main connection and survives, so a retried migration leaves a duplicate
 * history row that {@link DSQLConnection} reconciles afterward.
 *
 * <p>This wraps every transactional commit for the DSQL type, not just migrations (e.g.
 * schema-history table creation), so OCC conflicts on those commits are retried here too.
 *
 * <p>The schedule mirrors the connector's {@code OCCRetryConfig} defaults (100ms base delay, x2
 * multiplier, 25% jitter); the {@code dsql} configuration supplies the retry count and delay cap
 * (the count defaults to 0, so OCC retry is off unless {@code flyway.dsql.occMaxRetries} is set to a
 * positive value). Held here rather than in {@code OCCRetryConfig} so this decorator does not need
 * the connector on the classpath — see {@link DSQLOccErrors}.
 */
@CustomLog
class DSQLExecutionTemplate implements ExecutionTemplate {

    private static final long BASE_DELAY_MS = 100L;
    private static final double MULTIPLIER = 2.0d;
    private static final double JITTER_FACTOR = 0.25d;

    private final ExecutionTemplate delegate;
    private final int maxRetries;
    private final long maxDelayMs;

    DSQLExecutionTemplate(ExecutionTemplate delegate, int maxRetries, long maxDelayMs) {
        this.delegate = delegate;
        this.maxRetries = maxRetries;
        this.maxDelayMs = maxDelayMs;
    }

    @Override
    public <T> T execute(Callable<T> callback) {
        int attempt = 0;
        while (true) {
            try {
                return delegate.execute(callback);
            } catch (RuntimeException e) {
                if (!DSQLOccErrors.isOccError(e) || attempt >= maxRetries) {
                    throw e;
                }
                sleep(backoff(attempt));
                attempt++;
                LOG.info("Retrying migration transaction after Aurora DSQL OCC conflict (attempt "
                        + attempt + " of " + maxRetries + ")");
            }
        }
    }

    // Mirrors the connector's OCCRetry.calculateBackoff (package-private there, so it cannot be
    // reused): exponential base delay capped at maxDelayMs, plus up to JITTER_FACTOR of that delay.
    private long backoff(int attempt) {
        int exponent = Math.min(attempt, 31);
        double delay = Math.min(BASE_DELAY_MS * Math.pow(MULTIPLIER, exponent), maxDelayMs);
        double jitter = delay * ThreadLocalRandom.current().nextDouble() * JITTER_FACTOR;
        return (long) (delay + jitter);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new FlywayException("Aurora DSQL OCC retry interrupted", ie);
        }
    }
}
