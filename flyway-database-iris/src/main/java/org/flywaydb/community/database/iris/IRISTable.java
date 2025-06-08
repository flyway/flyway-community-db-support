/*-
 * ========================LICENSE_START=================================
 * flyway-database-iris
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
package org.flywaydb.community.database.iris;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class IRISTable extends Table<IRISDatabase, IRISSchema> {

    public static JdbcTemplate lockedJdbcTemplate = null;
    public static String lockedTable = null;
    public static AtomicInteger totalLocks = new AtomicInteger(0);

    /**
     * @param jdbcTemplate The JDBC template for communicating with the DB.
     * @param database     The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    public IRISTable(JdbcTemplate jdbcTemplate, IRISDatabase database, IRISSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForBoolean("SELECT DECODE((select 1 from %dictionary.compiledclass where SqlSchemaName = ? and SqlTableName = ?), 1, 1, 0)", schema.getName(), name);
    }

    @Override
    protected void doLock() throws SQLException {
        try {
            if (lockedJdbcTemplate == null && lockedTable == null) {
                lockedJdbcTemplate = jdbcTemplate;
                lockedTable = database.quote(schema.getName(), name);
            }
            if (acquireLock()) {
                jdbcTemplate.execute("LOCK TABLE " + lockedTable + " IN EXCLUSIVE MODE");
                totalLocks.incrementAndGet();
            } else {
                jdbcTemplate.execute("LOCK TABLE " + lockedTable + " IN EXCLUSIVE MODE");
                jdbcTemplate.execute("UNLOCK " + lockedTable + " IN EXCLUSIVE MODE");
                totalLocks.set(0);
                retry(15000);
            }
        } catch (SQLException sqlException) {
            if (unsuccessfulLockAcquiring()) {
                totalLocks.decrementAndGet();
            }
            retry(10000);
        }
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("SET OPTION COMPILEMODE = NOCHECK");
        jdbcTemplate.execute("DROP TABLE " + database.quote(schema.getName(), name) + " CASCADE");
    }

    private boolean acquireLock() {
        return totalLocks.get() >= 0;
    }

    private boolean unsuccessfulLockAcquiring() {
        return totalLocks.get() == 0;
    }

    private void retry(long backoffTime) throws SQLException {
        try {
            Thread.sleep(backoffTime);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            this.doLock();
        }
    }
}
