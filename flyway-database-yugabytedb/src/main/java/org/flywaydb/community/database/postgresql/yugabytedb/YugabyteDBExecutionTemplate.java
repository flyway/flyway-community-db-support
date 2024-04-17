package org.flywaydb.community.database.postgresql.yugabytedb;

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.strategy.RetryStrategy;
import org.flywaydb.core.internal.util.FlywayDbWebsiteLinks;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.Callable;

@CustomLog
public class YugabyteDBExecutionTemplate {

    private final JdbcTemplate jdbcTemplate;
    private final String tableName;
    private final HashMap<String, Boolean> tableEntries = new HashMap<>();


    YugabyteDBExecutionTemplate(JdbcTemplate jdbcTemplate, String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableName = tableName;
    }

    public <T> T execute(Callable<T> callable) {
        Exception error = null;
        try {
            lock();
            return callable.call();
        } catch (RuntimeException e) {
            error = e;
            throw e;
        } catch (Exception e) {
            error = e;
            throw new FlywayException(e);
        } finally {
            unlock(error);
        }
    }

    private void lock() throws SQLException {
        RetryStrategy strategy = new RetryStrategy();
        strategy.doWithRetries(this::tryLock, "Interrupted while attempting to acquire lock through SELECT ... FOR UPDATE",
                "Number of retries exceeded while attempting to acquire lock through SELECT ... FOR UPDATE. " +
                "Configure the number of retries with the 'lockRetryCount' configuration option: " + FlywayDbWebsiteLinks.LOCK_RETRY_COUNT);

    }

    private boolean tryLock() {
        Exception exception = null;
        boolean txStarted = false, success = false;
        Statement statement = null;
        try {
            statement = jdbcTemplate.getConnection().createStatement();

            if (!tableEntries.containsKey(tableName)) {
                try {
                    statement.executeUpdate("INSERT INTO "
                            + YugabyteDBDatabase.LOCK_TABLE_NAME
                            + " VALUES ('" + tableName + "', 'false')");
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

            boolean locked;
            String selectForUpdate = "SELECT locked FROM "
                    + YugabyteDBDatabase.LOCK_TABLE_NAME
                    + " WHERE table_name = '"
                    + tableName
                    + "' FOR UPDATE";
            String updateLocked = "UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME
                    + " SET locked = true WHERE table_name = '"
                    + tableName + "'";

            statement.execute("BEGIN");
            txStarted = true;
            ResultSet rs = statement.executeQuery(selectForUpdate);
            if (rs.next()) {
                locked = rs.getBoolean("locked");

                if (locked) {
                    statement.execute("COMMIT");
                    txStarted = false;
                    LOG.debug(Thread.currentThread().getName() + "> Another Flyway operation is in progress. Allowing it to complete");
                } else {
                    LOG.debug(Thread.currentThread().getName() + "> Setting locked = true");
                    statement.executeUpdate(updateLocked);
                    success = true;
                }
            } else {
                // For some reason the record was not found, retry
                tableEntries.remove(tableName);
            }

        } catch (SQLException e) {
            LOG.warn(Thread.currentThread().getName() + "> Unable to perform lock action, SQLState: " + e.getSQLState());
            if (!"40001".equalsIgnoreCase(e.getSQLState())) {
                exception = new FlywaySqlException("Unable to perform lock action", e);
                throw (FlywaySqlException) exception;
            } // else retry
        } finally {
            if (txStarted) {
                try {
                    statement.execute("COMMIT");
                    LOG.debug(Thread.currentThread().getName() + "> Completed the tx to set locked = true");
                } catch (SQLException e) {
                    if (exception == null) {
                        throw new FlywaySqlException("Failed to commit the tx to set locked = true", e);
                    }
                    LOG.warn(Thread.currentThread().getName() + "> Failed to commit the tx to set locked = true: " + e);
                }
            }
        }
        return success;
    }

    private void unlock(Exception rethrow) {
        Statement statement = null;
        try {
            statement = jdbcTemplate.getConnection().createStatement();
            statement.execute("BEGIN");
            ResultSet rs = statement.executeQuery("SELECT locked FROM " + YugabyteDBDatabase.LOCK_TABLE_NAME + " WHERE table_name = '" + tableName + "' FOR UPDATE");

            if (rs.next()) {
                boolean locked = rs.getBoolean("locked");
                if (locked) {
                    statement.executeUpdate("UPDATE " + YugabyteDBDatabase.LOCK_TABLE_NAME + " SET locked = false WHERE table_name = '" + tableName + "'");
                } else {
                    // Unexpected. This may happen only when callable took too long to complete
                    // and another thread forcefully reset it.
                    String msg = "Unlock failed but the Flyway operation may have succeeded. Check your Flyway operation before re-trying";
                    LOG.warn(Thread.currentThread().getName() + "> " + msg);
                    if (rethrow == null) {
                        throw new FlywayException(msg);
                    }
                }
            }
        } catch (SQLException e) {
            if (rethrow == null) {
                rethrow = new FlywayException("Unable to perform unlock action", e);
                throw (FlywaySqlException) rethrow;
            }
            LOG.warn("Unable to perform unlock action " + e);
        } finally {
            try {
                statement.execute("COMMIT");
                LOG.debug(Thread.currentThread().getName() + "> Completed the tx to set locked = false");
            } catch (SQLException e) {
                if (rethrow == null) {
                    throw new FlywaySqlException("Failed to commit unlock action", e);
                }
                LOG.warn("Failed to commit unlock action: " + e);
            }
        }
    }

}
