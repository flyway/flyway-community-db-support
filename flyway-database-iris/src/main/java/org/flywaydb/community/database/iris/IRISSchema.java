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

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

public class IRISSchema extends Schema<IRISDatabase, IRISTable> {
    /**
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public IRISSchema(JdbcTemplate jdbcTemplate, IRISDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() {
        return true;
    }

    @Override
    protected boolean doEmpty() {
        return false;
    }

    @Override
    protected void doCreate() {
        throw new UnsupportedOperationException("InterSystems IRIS does not support schema creation. Schema not created");
    }

    @Override
    protected void doDrop() {
        throw new UnsupportedOperationException("InterSystems IRIS does not support dropping schemas. Schema not dropped");
    }

    @Override
    protected void doClean() {
        Stream.of(allTables()).forEach(Table::drop);
    }

    @Override
    protected IRISTable[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "SELECT SqlTableName from %dictionary.compiledclass where SqlSchemaName = ?", name);
        return tableNames.stream().map(tableName -> new IRISTable(jdbcTemplate, database, this, tableName)).toArray(IRISTable[]::new);
    }

    @Override
    public IRISTable getTable(String tableName) {
        return new IRISTable(jdbcTemplate, database, this, tableName);
    }
}
