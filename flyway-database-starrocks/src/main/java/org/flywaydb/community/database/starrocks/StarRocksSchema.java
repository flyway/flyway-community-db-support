/*-
 * ========================LICENSE_START=================================
 * flyway-database-starrocks
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

package org.flywaydb.community.database.starrocks;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StarRocksSchema extends Schema<StarRocksDatabase, StarRocksTable> {

    public StarRocksSchema(JdbcTemplate jdbcTemplate, StarRocksDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?", name) == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.executeStatement("CREATE DATABASE " + database.quote(name));
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.executeStatement("DROP DATABASE " + database.quote(name));
    }

    @Override
    protected void doClean() throws SQLException {
        // Drop all views first
        for (String viewName : allViews()) {
            jdbcTemplate.executeStatement("DROP VIEW IF EXISTS " + database.quote(name, viewName));
        }

        // Drop all tables
        for (Table table : allTables()) {
            table.drop();
        }
    }

    private List<String> allViews() throws SQLException {
        List<Map<String, String>> results = jdbcTemplate.queryForList(
                "SELECT TABLE_NAME FROM information_schema.views WHERE TABLE_SCHEMA = ?", name);
        List<String> views = new ArrayList<>();
        for (Map<String, String> row : results) {
            views.add(row.get("TABLE_NAME"));
        }
        return views;
    }

    @Override
    protected StarRocksTable[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'", name);
        StarRocksTable[] tables = new StarRocksTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new StarRocksTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new StarRocksTable(jdbcTemplate, database, this, tableName);
    }
}
