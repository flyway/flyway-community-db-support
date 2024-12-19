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

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class TimeplusSchema extends Schema<TimeplusDatabase, TimeplusTable> {

    private static final String DEFAULT_SCHEMA = "default";

    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public TimeplusSchema(JdbcTemplate jdbcTemplate, TimeplusDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        TimeplusConnection systemConnection = database.getSystemConnection();
        int i = systemConnection.getJdbcTemplate().queryForInt("SELECT COUNT() FROM system.databases WHERE name = ?", name);
        return i > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        TimeplusConnection systemConnection = database.getSystemConnection();
        int objectCount = systemConnection.getJdbcTemplate().queryForInt("SELECT COUNT() FROM system.tables WHERE database = ?", name) + getObjectCount("FUNCTION")
                + getObjectCount("FORMAT SCHEMA");
        return objectCount == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        TimeplusConnection systemConnection = database.getSystemConnection();
        String clusterName = database.getClusterName();
        boolean isClustered = StringUtils.hasText(clusterName);
        systemConnection.getJdbcTemplate().executeStatement("CREATE DATABASE " + database.quote(name) + (false && isClustered ? (" ON CLUSTER " + clusterName) : ""));
    }

    @Override
    protected void doDrop() throws SQLException {
        if (database.getMainConnection().getCurrentSchemaNameOrSearchPath().equals(name)) {
            database.getMainConnection().doChangeCurrentSchemaOrSearchPathTo(Optional.ofNullable(database.getConfiguration().getDefaultSchema()).orElse(DEFAULT_SCHEMA));
        }
        String clusterName = database.getClusterName();
        boolean isClustered = StringUtils.hasText(clusterName);
        jdbcTemplate.executeStatement("DROP DATABASE " + database.quote(name) + (isClustered ? (" ON CLUSTER " + clusterName) : ""));
    }

    @Override
    protected void doClean() throws SQLException {
        // TODO: this will drop streams, views, materialized views, when there are dependencies, DROP could fail.
        for (TimeplusTable table : allTables()) {
            table.drop();
        }
        for (String dropStatement : generateDropStatements("FORMAT SCHEMA")) {
            jdbcTemplate.execute(dropStatement);
        }

        for (String dropStatement : generateDropStatements("FUNCTION")) {
            jdbcTemplate.execute(dropStatement);
        }
    }

    @Override
    protected TimeplusTable[] doAllTables() throws SQLException {
        TimeplusConnection systemConnection = database.getSystemConnection();
        return systemConnection.getJdbcTemplate().queryForStringList("SELECT name FROM system.tables WHERE database = ?", name)
                .stream()
                .map(this::getTable)
                .toArray(TimeplusTable[]::new);
    }

    @Override
    public TimeplusTable getTable(String tableName) {
        return new TimeplusTable(jdbcTemplate, database, this, tableName);
    }

    private int getObjectCount(String objectType) throws SQLException {
        return jdbcTemplate.query("SHOW " + objectType + "S", rs -> 1).size();
    }

    private List<String> generateDropStatements(final String objectType) throws SQLException {
        return jdbcTemplate.query("SHOW " + objectType + "S", rs -> {
            String resName = rs.getString("name");
            return "DROP " + objectType + " " + database.quote(resName);
        });
    }
}
