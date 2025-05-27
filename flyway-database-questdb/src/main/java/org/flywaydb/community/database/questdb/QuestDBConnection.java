/*-
 * ========================LICENSE_START=================================
 * flyway-database-questdb
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
package org.flywaydb.community.database.questdb;

import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;

public class QuestDBConnection extends Connection<QuestDBDatabase> {

    private final QuestDBSchema schema = new QuestDBSchema(jdbcTemplate, database, "default");

    QuestDBConnection(QuestDBDatabase database, java.sql.Connection connection) {
        super(database, connection);
    }

    @Override
    public Schema<QuestDBDatabase, QuestDBTable> getSchema(String name) {
        return schema;
    }

    @Override
    protected void doRestoreOriginalState() {
    }

    @Override
    public Schema<QuestDBDatabase, QuestDBTable> doGetCurrentSchema() {
        return schema;
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() {
        return "default";
    }

    @Override
    public void changeCurrentSchemaTo(final Schema schema) {
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(final String schema) {
    }
}
