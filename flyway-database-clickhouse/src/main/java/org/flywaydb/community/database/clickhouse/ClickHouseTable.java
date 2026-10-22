/*-
 * ========================LICENSE_START=================================
 * flyway-database-clickhouse
 * ========================================================================
 * Copyright (C) 2010 - 2025 Red Gate Software Ltd
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

package org.flywaydb.community.database.clickhouse;

import lombok.CustomLog;
import org.flywaydb.core.internal.database.InsertRowLock;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.List;

@CustomLog
public class ClickHouseTable extends Table<ClickHouseDatabase, ClickHouseSchema> {


    /**
     * When altering the table wait for all replicas to sync changes
     * @see <a href="https://clickhouse.com/docs/operations/settings/settings#mutations_sync">mutations_sync</a>
     */
    private static final String ALTER_TABLE_TEMPLATE = "ALTER TABLE %s %s SETTINGS mutations_sync=3;";

    private final InsertRowLock insertRowLock;

    /**
     * @param jdbcTemplate The JDBC template for communicating with the DB.
     * @param database The database-specific support.
     * @param schema The schema this table lives in.
     * @param name The name of the table.
     */
    public ClickHouseTable(JdbcTemplate jdbcTemplate, ClickHouseDatabase database, ClickHouseSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
        this.insertRowLock = new InsertRowLock(jdbcTemplate);
    }

    @Override
    protected void doDrop() throws SQLException {
        String clusterName = database.getClusterName();

        jdbcTemplate.executeStatement("DROP TABLE " + this + (StringUtils.hasText(clusterName) ? (" ON CLUSTER " + clusterName) : ""));
    }

    @Override
    protected boolean doExists() throws SQLException {
        ClickHouseConnection systemConnection = database.getSystemConnection();
        int count = systemConnection.getJdbcTemplate().queryForInt("SELECT COUNT() FROM system.tables WHERE database = ? AND name = ?", schema.getName(), name);
        return count > 0;
    }


    /**
     * This implementation leverages an insert-based locking strategy that is compatible with ClickHouse's
     * limitations (e.g., no standard transactional row-level locks limited DELETE support on MergeTree,
     * and no real Primary Key constraints). It is designed to support concurrent Flyway migrations across
     * distributed nodes or processes via insert timing and deterministic resolution of owners.
     *
     * <p>
     * Locking Strategy Overview:
     * <ul>
     *   <li>Each process attempting to acquire the lock inserts a row into the Flyway schema history table
     *       with a unique `version`, `description = 'flyway-lock'`, and a current timestamp.</li>
     *   <li>High-precision timestamps are captured using `now64(9)` which returns a DateTime64 with nanosecond precision.
     *       This timestamp is split across two columns:
     *       <ul>
     *         <li>`installed_on` (DateTime): stores the whole-second part via `toDateTime(now64(9))`, truncating subseconds</li>
     *         <li>`checksum` (Int32): stores the subsecond nanoseconds via `(now64(9) - toDateTime(now64(9))) * 1000000000`,
     *             extracting the fractional seconds and converting to nanoseconds as an integer</li>
     *       </ul>
     *       This split allows nanosecond-precision ordering while working within the table's existing schema.
     *   </li>
     *   <li>To determine the lock owner, the table is queried using `ORDER BY (installed_on, checksum, version)`
     *       and the first row is selected. This ensures consistent ordering across processes and helps resolve
     *       race conditions if two inserts occur within the same second.</li>
     *   <li>Unlocking is performed by deleting the specific lock row matching the unique `version`.</li>
     * </ul>
     * </p>
     */
    @Override
    protected void doLock() throws SQLException {

        if (lockDepth == 0) {

            // callback to update the lock row as the migration runs
            String updateLockStatement = ALTER_TABLE_TEMPLATE.formatted(this,
                    "UPDATE execution_time = dateDiff('second', installed_on, now())" +
                            " WHERE version = '?' AND description = '" + InsertRowLock.FLYWAY_LOCK_STRING + "'");


            // callback to delete locks that are older than the ? parameter. Need to combine
            // installed_on (the start time) and execution_time (run time in seconds)
            // set mutations sync
            String deleteExpiredLockStatement = ALTER_TABLE_TEMPLATE.formatted(this,
                    "DELETE WHERE description = '" + InsertRowLock.FLYWAY_LOCK_STRING + "'" +
                            " AND addSeconds(installed_on, execution_time) < parseDateTimeBestEffort('?')");


            // current_time has nanosecond precision, the subsecond value will be put into checksum
            // so that we have the full nanosecond date to handle races where two rows are inserted
            // at the same second
            String insertStatementTemplate = "INSERT INTO " + this +
                    "(installed_rank, version, description, type, script, checksum, installed_by, execution_time, success, installed_on)" +
                    " WITH now64(9) AS current_time" +
                    " SELECT" +
                    " ? AS installed_rank," +
                    " ? AS version," +
                    " ? AS description," +
                    " ? AS type," +
                    " ? AS script," +
                    " ? AS checksum," +
                    " ? AS installed_by," +
                    " ? AS execution_time," +
                    " ? AS success," +
                    " toDateTime(current_time) AS installed_on";

            insertRowLock.doLock(insertStatementTemplate,
                    updateLockStatement,
                    deleteExpiredLockStatement,
                    // put the current_time nanoseconds into the checksum column
                    "dateDiff('nanoseconds', toDateTime(current_time), current_time)",
                    database.getBooleanTrue(), (jdbcTemplate, insertStatement) -> {

                        String currentLockOwner = getCurrentLockOwner();
                        if (insertRowLock.getLockId().equals(currentLockOwner)) {
                            return true;
                        }
                        if (currentLockOwner != null) {
                            // owned by another migration

                            if (LOG.isDebugEnabled()) {
                                LOG.debug(insertRowLock.getLockId() + " unable to acquire lock on Flyway schema history table, already owned by " + currentLockOwner);
                            }
                            return false;
                        }


                        Results results = jdbcTemplate.executeStatement(insertStatement);

                        if (results.getException() != null) {
                            LOG.error(insertRowLock.getLockId() + " exception inserting Flyway lock", results.getException());
                            return false;
                        }

                        // TODO hack, sleep before querying so that if we have a race where two inserts happen at the
                        // same exact time the query for current will see both such that sorting can select the winner
                        try {
                            Thread.sleep(100);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                        currentLockOwner = getCurrentLockOwner();
                        if (insertRowLock.getLockId().equals(currentLockOwner)) {
                            // this row lock is the owner
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(insertRowLock.getLockId() + " acquired lock on Flyway schema history table");
                            }
                            return true;
                        }

                        // not the owner
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(insertRowLock.getLockId() + " attempted to acquire lock on Flyway schema history table, but already owned by " + currentLockOwner);
                        }
                        return false;
                    });
        }
    }

    @Override
    protected void doUnlock() throws SQLException {
        if (lockDepth == 1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(insertRowLock.getLockId() + " unlocking Flyway schema history table");
            }
            insertRowLock.doUnlock(ALTER_TABLE_TEMPLATE.formatted( this , "DELETE WHERE version = '?' AND description = '" + InsertRowLock.FLYWAY_LOCK_STRING + "'"));
        }
    }

    private String getCurrentLockOwner() {
        // select oldest owner based on full nanosecond date64 installed time (installed_on + checksum).
        // Additionally order by version (unique string for this migration) to resolve edge cases
        // where two inserts had the same nanosecond time.
        Results results = jdbcTemplate.executeStatement( "SELECT version FROM " + this +
                " WHERE description = '" + InsertRowLock.FLYWAY_LOCK_STRING + "'" +
                " ORDER BY (installed_on, checksum, version) ASC" +
                " LIMIT 1" +
                " SETTINGS select_sequential_consistency=1");

        for (Result result : results.getResults()) {
            List<List<String>> data = result.data();
            if (data == null || data.isEmpty()) {
                continue;
            }
            return data.get(0).get(0);
        }
        return null;
    }
}
