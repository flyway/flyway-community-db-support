package org.flywaydb.community.database.postgresql.yugabytedb;

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.FlywayDbWebsiteLinks;
import org.flywaydb.core.internal.util.SqlCallable;

import java.sql.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

@CustomLog
public class YugabyteDBExecutionTemplate {

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;
    private static final Map<String, Boolean> tableEntries = new ConcurrentHashMap<>();
    private static final Random random = new Random();

    YugabyteDBExecutionTemplate(JdbcTemplate jdbcTemplate, String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    public <T> T execute(Callable<T> callable) {
        Exception error = null;
        long lockId = 0;
        try {
            lockId = lock();
            return callable.call();
        } catch (RuntimeException e) {
            error = e;
            throw e;
        } catch (Exception e) {
            error = e;
            throw new FlywayException(e);
        } finally {
            if (lockId != 0) {
                unlock(lockId, error);
            }
        }
    }

    private long lock() throws SQLException {
        YBRetryStrategy strategy = new YBRetryStrategy();
        return strategy.doWithRetries(this::tryLock, "Interrupted while attempting to acquire lock through SELECT ... FOR UPDATE",
                "Number of retries exceeded while attempting to acquire lock through SELECT ... FOR UPDATE. " +
                "Configure the number of retries with the 'lockRetryCount' configuration option: " + FlywayDbWebsiteLinks.LOCK_RETRY_COUNT);

    }

    private long tryLock() {
        Exception exception = null;
        boolean txStarted = false;
        long lockIdToBeReturned = 0;
        Statement statement = null;
        try {
            statement = jdbcTemplate.getConnection().createStatement();

            if (!tableEntries.containsKey(tableName)) {
                try {
                    statement.executeUpdate("INSERT INTO "
                            + YugabyteDBDatabase.LOCK_TABLE_NAME
                            + " VALUES ('" + tableName + "', 0)");
                    tableEntries.put(tableName, true);
                    LOG.info(Thread.currentThread().getName() + "> Inserted a token row for " + tableName + " in " + YugabyteDBDatabase.LOCK_TABLE_NAME);
                } catch (SQLException e) {
                    if ("23505".equals(e.getSQLState())) {
                        // 23505 == UNIQUE_VIOLATION
                        LOG.debug(Thread.currentThread().getName() + "> Token row already added for " + tableName);
                    } else {
                        throw new FlywaySqlException("Could not add token row for " + tableName + " in table " + YugabyteDBDatabase.LOCK_TABLE_NAME, e);
                    }
                }
            }

            long lockIdRead = 0;
            String selectForUpdate = "SELECT lock_id FROM "
                    + YugabyteDBDatabase.LOCK_TABLE_NAME
                    + " WHERE table_name = '"
                    + tableName
                    + "' FOR UPDATE";

            statement.execute("BEGIN");
            txStarted = true;
            ResultSet rs = statement.executeQuery(selectForUpdate);
            if (rs.next()) {
                lockIdRead = rs.getLong("lock_id");

                if (lockIdRead != 0) {
                    statement.execute("COMMIT");
                    txStarted = false;
                    LOG.debug(Thread.currentThread().getName() + "> Another Flyway operation is in progress. Allowing it to complete");
                } else {
                    lockIdToBeReturned = random.nextLong();
                    String updateLockId = "UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME
                            + " SET lock_id = " + lockIdToBeReturned + " WHERE table_name = '"
                            + tableName + "'";
                    LOG.debug(Thread.currentThread().getName() + "> Setting lock_id = " + lockIdToBeReturned);
                    statement.executeUpdate(updateLockId);
                }
            } else {
                // For some reason the record was not found, retry
                tableEntries.remove(tableName);
            }

        } catch (SQLException e) {
            LOG.debug(Thread.currentThread().getName() + "> Unable to perform lock action, SQLState: " + e.getSQLState());
            if (!"40001".equalsIgnoreCase(e.getSQLState())) {
                exception = new FlywaySqlException("Unable to perform lock action", e);
                throw (FlywaySqlException) exception;
            } // else retry
        } finally {
            if (txStarted) {
                try {
                    statement.execute("COMMIT");
                    // lock_id may not be set if there is exception in select for update
                    LOG.debug(Thread.currentThread().getName() + "> Completed the tx to attempt to set lock_id");
                } catch (SQLException e) {
                    if (exception == null) {
                        throw new FlywaySqlException("Failed to commit the tx to set lock_id ", e);
                    }
                    LOG.warn(Thread.currentThread().getName() + "> Failed to commit the tx to set lock_id: " + e);
                }
            }
        }
        return lockIdToBeReturned;
    }

    private void unlock(long prevLockId, Exception rethrow) {
        Statement statement = null;
        try {
            statement = jdbcTemplate.getConnection().createStatement();
            statement.execute("BEGIN");
            ResultSet rs = statement.executeQuery("SELECT lock_id FROM " + YugabyteDBDatabase.LOCK_TABLE_NAME + " WHERE table_name = '" + tableName + "' FOR UPDATE");

            if (rs.next()) {
                long lockId = rs.getLong("lock_id");
                if (lockId == prevLockId) {
                    statement.executeUpdate("UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET lock_id = 0 WHERE table_name = '" + tableName + "'");
                } else {
                    // Unexpected. This may happen only when callable took too long to complete
                    // and another thread forcefully reset it.
                    String msgLock = "Expected and actual lock_id mismatch. Expected: " + prevLockId + ", Actual: " + lockId;
                    String msg = "Unlock failed but the Flyway operation may have succeeded. Check your Flyway operation before re-trying";
                    LOG.warn(Thread.currentThread().getName() + "> " + msg + "\n" + msgLock);
                    if (rethrow == null) {
                        throw new FlywayException(msg);
                    }
                }
            }
        } catch (SQLException e) {
            if (rethrow == null) {
                rethrow = new FlywaySqlException("Unable to perform unlock action for lock_id " + prevLockId, e);
                throw (FlywaySqlException) rethrow;
            }
            LOG.warn("Unable to perform unlock action for lock_id " + prevLockId + ": " + e);
        } finally {
            try {
                statement.execute("COMMIT");
                LOG.debug(Thread.currentThread().getName() + "> Completed the tx to reset lock_id " + prevLockId);
            } catch (SQLException e) {
                if (rethrow == null) {
                    throw new FlywaySqlException("Failed to commit unlock action for lock_id " + prevLockId, e);
                }
                LOG.warn("Failed to commit unlock action for lock_id " + prevLockId + ": " + e);
            }
        }
    }

    public static class YBRetryStrategy {
        private static int numberOfRetries = 50;
        private static boolean unlimitedRetries;
        private int numberOfRetriesRemaining;

        public YBRetryStrategy() {
            this.numberOfRetriesRemaining = numberOfRetries;
        }

        public static void setNumberOfRetries(int retries) {
            numberOfRetries = retries;
            unlimitedRetries = retries < 0;
        }

        private boolean hasMoreRetries() {
            return unlimitedRetries || this.numberOfRetriesRemaining > 0;
        }

        private void nextRetry() {
            if (!unlimitedRetries) {
                --this.numberOfRetriesRemaining;
            }
        }

        private int nextWaitInMilliseconds() {
            return 1000;
        }

        public long doWithRetries(SqlCallable<Long> callable, String interruptionMessage, String retriesExceededMessage) throws SQLException {
            long id = 0;
            while(id == 0) {
                id = callable.call();
                try {
                    Thread.sleep(this.nextWaitInMilliseconds());
                } catch (InterruptedException e) {
                    throw new FlywayException(interruptionMessage, e);
                }
                if (!this.hasMoreRetries()) {
                    throw new FlywayException(retriesExceededMessage);
                }
                this.nextRetry();
            }
            return id;
        }
    }
}
