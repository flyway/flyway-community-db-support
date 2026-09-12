/*-
 * ========================LICENSE_START=================================
 * flyway-database-frostlake
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
package org.flywaydb.community.database.frostlake;

import lombok.CustomLog;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@CustomLog
public class FrostlakeDatabase extends Database<FrostlakeConnection> {
    /**
     * Whether quoted identifiers are treated in a case-insensitive way. Defaults to false. See
     * https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#controlling-case-using-the-quoted-identifiers-ignore-case-parameter
     */
    private final boolean quotedIdentifiersIgnoreCase;

    public FrostlakeDatabase(final Configuration configuration,
        final JdbcConnectionFactory jdbcConnectionFactory,
        final StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);

        // There will be issues if the Flyway schema history table was created while this option was set false
        // (it is set either at the account level, or the individual session level) and it is subsequently set true.
        quotedIdentifiersIgnoreCase = getQuotedIdentifiersIgnoreCase(jdbcTemplate);
        LOG.info("QUOTED_IDENTIFIERS_IGNORE_CASE option is " + quotedIdentifiersIgnoreCase);
    }

    private static boolean getQuotedIdentifiersIgnoreCase(final JdbcTemplate jdbcTemplate) {
        try {
            // Attempt query; an engine that does not expose the parameter answers with no rows,
            // which means the default (false).
            final List<Map<String, String>> result = jdbcTemplate.queryForList(
                "SHOW PARAMETERS LIKE 'QUOTED_IDENTIFIERS_IGNORE_CASE'");
            if (result.isEmpty()) {
                return false;
            }
            final Map<String, String> row = result.get(0);
            return "TRUE".equals(row.get("value").toUpperCase(Locale.ENGLISH));
        } catch (SQLException e) {
            LOG.warn("Could not query for parameter QUOTED_IDENTIFIERS_IGNORE_CASE.");
            return false;
        }
    }

    @Override
    protected FrostlakeConnection doGetConnection(final Connection connection) {
        return new FrostlakeConnection(this, connection);
    }

    @Override
    public void ensureSupported(final Configuration configuration) {
        // Frostlake versions independently of Snowflake's major numbers; every version speaks the
        // dialect this module targets.
    }

    @Override
    public String getRawCreateScript(final Table table, final boolean baseline) {
        // CAUTION: Quotes are optional around column names without underscores; but without them, Frostlake will
        // uppercase the column name leading to SELECTs failing.
        return "CREATE TABLE "
            + table
            + " (\n"
            + quote("installed_rank")
            + " NUMBER(38,0) NOT NULL,\n"
            + quote("version")
            + " VARCHAR(50),\n"
            + quote("description")
            + " VARCHAR(200),\n"
            + quote("type")
            + " VARCHAR(20) NOT NULL,\n"
            + quote("script")
            + " VARCHAR(1000) NOT NULL,\n"
            + quote("checksum")
            + " NUMBER(38,0),\n"
            + quote("installed_by")
            + " VARCHAR(100) NOT NULL,\n"
            + quote("installed_on")
            + " TIMESTAMP_LTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),\n"
            + quote("execution_time")
            + " NUMBER(38,0) NOT NULL,\n"
            + quote("success")
            + " BOOLEAN NOT NULL,\n"
            + "primary key ("
            + quote("installed_rank")
            + "));\n"
            +

            (baseline ? getBaselineStatement(table) + ";\n" : "");
    }

    @Override
    public String getSelectStatement(final Table table) {
        // CAUTION: Quotes are optional around column names without underscores; but without them, Frostlake will
        // uppercase the column name. In data readers, the column name is case sensitive.
        return "SELECT "
            + quote("installed_rank")
            + ","
            + quote("version")
            + ","
            + quote("description")
            + ","
            + quote("type")
            + ","
            + quote("script")
            + ","
            + quote("checksum")
            + ","
            + quote("installed_on")
            + ","
            + quote("installed_by")
            + ","
            + quote("execution_time")
            + ","
            + quote("success")
            + " FROM "
            + table
            + " WHERE "
            + quote("installed_rank")
            + " > ?"
            + " ORDER BY "
            + quote("installed_rank");
    }

    @Override
    public String getInsertStatement(final Table table) {
        // CAUTION: Quotes are optional around column names without underscores; but without them, Frostlake will
        // uppercase the column name.
        return "INSERT INTO "
            + table
            + " ("
            + quote("installed_rank")
            + ", "
            + quote("version")
            + ", "
            + quote("description")
            + ", "
            + quote("type")
            + ", "
            + quote("script")
            + ", "
            + quote("checksum")
            + ", "
            + quote("installed_by")
            + ", "
            + quote("execution_time")
            + ", "
            + quote("success")
            + ")"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "true";
    }

    @Override
    public String getBooleanFalse() {
        return "false";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

}
