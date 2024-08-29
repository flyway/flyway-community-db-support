/*
 * Copyright (C) Red Gate Software Ltd 2010-2022
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
package org.flywaydb.community.database.db2z;

import java.sql.SQLException;
import lombok.CustomLog;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

/**
 * Db2-specific table.
 */
@CustomLog
public class DB2ZTable extends Table<DB2ZDatabase, DB2ZSchema> {

    /**
     * Creates a new Db2 table.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param schema       The schema this table lives in.
     * @param name         The name of the table.
     */
    DB2ZTable(JdbcTemplate jdbcTemplate, DB2ZDatabase database, DB2ZSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
		
		String dbName = jdbcTemplate.queryForString("SELECT DBNAME FROM SYSIBM.SYSTABLES WHERE NAME=? AND CREATOR=?", this.getName(), this.getSchema().getName());
        String tableSpaceName = jdbcTemplate.queryForString("SELECT TSNAME FROM SYSIBM.SYSTABLES WHERE NAME=? AND CREATOR=?", this.getName(), this.getSchema().getName());
        //Use sqlid as creator with tablespace. When sqlid is not set, implicitly use schema name for sqlid
        String sqlId = (database.getSqlId() == "") ? this.getSchema().getName() : database.getSqlId();
        String implicit = jdbcTemplate.queryForString("SELECT IMPLICIT FROM SYSIBM.SYSTABLESPACE WHERE DBNAME=? AND CREATOR=? AND NAME=?", dbName, sqlId, tableSpaceName);
		String tableSpaceType = jdbcTemplate.queryForString("SELECT TYPE FROM SYSIBM.SYSTABLESPACE WHERE DBNAME=? AND CREATOR=? AND NAME=?", dbName, sqlId, tableSpaceName);

        if (implicit == null || implicit.isEmpty())  {
            LOG.debug("Nothing to drop because table " + this.getName() + " does exist on tablespace " + tableSpaceName + " but with creator other than " + sqlId);
        } else {
            if (implicit.equals("N") && (tableSpaceType.equals("G") || tableSpaceType.equals("R"))) {
				//Tablespace will be dropped by DB2ZSchema.doClean()
                LOG.debug("Table '" + this + "' cannot be dropped directly (tableSpaceName=" + tableSpaceName + ", implicit=" + implicit + ", tableSpaceType=" + tableSpaceType + ")");
            } else {
                LOG.debug("Dropping table " + this + " ...");
                jdbcTemplate.execute("DROP TABLE " + this);			
            }
        }
    }

    @Override
    protected boolean doExists() throws SQLException {
        return exists(null, schema, name);
    }

    @Override
    protected void doLock() throws SQLException {
        jdbcTemplate.update("lock table " + this + " in exclusive mode");
    }
}
