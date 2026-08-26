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
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.database.postgresql.PostgreSQLSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Aurora DSQL schema implementation for Flyway.
 *
 * <p>Aurora DSQL allows only one DDL statement per transaction, so {@link #doClean()}
 * drops views then tables one at a time with autocommit enabled.
 */
@CustomLog
public class DSQLSchema extends PostgreSQLSchema {

    public DSQLSchema(JdbcTemplate jdbcTemplate, DSQLDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    public Table getTable(String tableName) {
        return new DSQLTable(jdbcTemplate, (DSQLDatabase) database, this, tableName);
    }

    @Override
    protected void doClean() throws SQLException {
        Connection conn = jdbcTemplate.getConnection();
        boolean originalAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(true);

            for (String view : getViews(conn)) {
                String dropSql = "DROP VIEW IF EXISTS " + database.quote(name, view);
                LOG.debug("Dropping view: " + dropSql);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(dropSql);
                }
            }

            for (Table table : allTables()) {
                String dropSql = "DROP TABLE IF EXISTS " + database.quote(name, table.getName());
                LOG.debug("Dropping table: " + dropSql);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(dropSql);
                }
            }
        } finally {
            conn.setAutoCommit(originalAutoCommit);
        }
    }

    private List<String> getViews(Connection conn) throws SQLException {
        List<String> views = new ArrayList<>();
        String sql = "SELECT table_name FROM information_schema.views WHERE table_schema = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    views.add(rs.getString(1));
                }
            }
        }
        return views;
    }
}
