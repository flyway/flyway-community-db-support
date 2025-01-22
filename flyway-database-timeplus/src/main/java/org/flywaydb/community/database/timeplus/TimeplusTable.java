/*-
 * ========================LICENSE_START=================================
 * flyway-database-timeplus
 * ========================================================================
 * Copyright (C) 2010 - 2024 Red Gate Software Ltd
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
/*
 * Copyright (C) Red Gate Software Ltd 2010-2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.community.database.timeplus;

import lombok.CustomLog;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;

@CustomLog
public class TimeplusTable extends Table<TimeplusDatabase, TimeplusSchema> {
    /**
     * @param jdbcTemplate The JDBC template for communicating with the DB.
     * @param database The database-specific support.
     * @param schema The schema this table lives in.
     * @param name The name of the table.
     */
    public TimeplusTable(JdbcTemplate jdbcTemplate, TimeplusDatabase database, TimeplusSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        String clusterName = database.getClusterName();

        jdbcTemplate.executeStatement("DROP STREAM " + this + (StringUtils.hasText(clusterName) ? (" ON CLUSTER " + clusterName) : ""));
    }

    @Override
    protected boolean doExists() throws SQLException {
        TimeplusConnection systemConnection = database.getSystemConnection();
        int count = systemConnection.getJdbcTemplate().queryForInt("SELECT COUNT() FROM system.tables WHERE database = ? AND name = ?", schema.getName(), name);
        return count > 0;
    }

    @Override
    protected void doLock() throws SQLException {
        LOG.debug("Unable to lock " + this + " as Timeplus does not support locking. No concurrent migration supported.");
    }
}
