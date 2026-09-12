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

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.ExecutionTemplate;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DSQLExecutionTemplateTest {

    // Fake delegate template that simulates flyway-core's TransactionalExecutionTemplate:
    // it invokes the callback, then "commits" — surfacing pre-scripted commit outcomes as the
    // FlywaySqlException that a real commit-time OCC conflict throws (thrown at the transaction
    // boundary, NOT inside a single statement).
    private static class FakeTemplate implements ExecutionTemplate {
        final AtomicInteger executes = new AtomicInteger();
        private final RuntimeException[] commitOutcomes; // null entry => commit succeeds

        FakeTemplate(RuntimeException... commitOutcomes) {
            this.commitOutcomes = commitOutcomes;
        }

        @Override
        public <T> T execute(Callable<T> callback) {
            int i = executes.getAndIncrement();
            try {
                T result = callback.call();
                RuntimeException outcome = i < commitOutcomes.length ? commitOutcomes[i] : null;
                if (outcome != null) {
                    throw outcome; // simulate connection.commit() failing
                }
                return result;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static FlywaySqlException occCommitFailure() {
        // Mirrors TransactionalExecutionTemplate wrapping a commit SQLException.
        return new FlywaySqlException("Unable to commit transaction",
                new SQLException("catalog conflict", "OC001"));
    }

    private static FlywaySqlException nonOccCommitFailure() {
        return new FlywaySqlException("Unable to commit transaction",
                new SQLException("syntax error", "42601"));
    }

    // fast backoff: a 1ms delay cap keeps sleeps negligible for tests
    private DSQLExecutionTemplate template(FakeTemplate delegate, int maxRetries) {
        return new DSQLExecutionTemplate(delegate, maxRetries, 1L);
    }

    @Test
    void retriesCommitTimeOccThenSucceeds() {
        FakeTemplate delegate = new FakeTemplate(occCommitFailure(), null);
        String result = template(delegate, 6).execute(() -> "ok");
        assertThat(result).isEqualTo("ok");
        assertThat(delegate.executes.get()).isEqualTo(2); // initial + 1 retry
    }

    @Test
    void doesNotRetryNonOccCommitFailure() {
        FakeTemplate delegate = new FakeTemplate(nonOccCommitFailure(), null);
        assertThatThrownBy(() -> template(delegate, 6).execute(() -> "ok"))
                .isInstanceOf(FlywaySqlException.class);
        assertThat(delegate.executes.get()).isEqualTo(1);
    }

    @Test
    void exhaustsRetriesThenRethrows() {
        FakeTemplate delegate = new FakeTemplate(
                occCommitFailure(), occCommitFailure(), occCommitFailure(), occCommitFailure());
        assertThatThrownBy(() -> template(delegate, 2).execute(() -> "ok"))
                .isInstanceOf(FlywaySqlException.class);
        assertThat(delegate.executes.get()).isEqualTo(3); // initial + 2 retries
    }

    @Test
    void zeroRetriesExecutesOnce() {
        FakeTemplate delegate = new FakeTemplate(occCommitFailure(), null);
        assertThatThrownBy(() -> template(delegate, 0).execute(() -> "ok"))
                .isInstanceOf(FlywaySqlException.class);
        assertThat(delegate.executes.get()).isEqualTo(1);
    }

    @Test
    void passesThroughSuccessWithoutRetry() {
        FakeTemplate delegate = new FakeTemplate();
        String result = template(delegate, 6).execute(() -> "done");
        assertThat(result).isEqualTo("done");
        assertThat(delegate.executes.get()).isEqualTo(1);
    }

    @Test
    void interruptDuringBackoffThrowsFlywayException() {
        FakeTemplate delegate = new FakeTemplate(occCommitFailure(), null);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> template(delegate, 6).execute(() -> "ok"))
                    .isInstanceOf(FlywayException.class)
                    .hasMessageContaining("interrupted");
        } finally {
            // clear the interrupt flag so it doesn't leak into other tests
            Thread.interrupted();
        }
    }
}
