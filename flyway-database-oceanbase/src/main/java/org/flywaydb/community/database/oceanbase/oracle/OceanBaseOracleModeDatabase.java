/*-
 * ========================LICENSE_START=================================
 * flyway-database-oracle
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
package org.flywaydb.community.database.oceanbase.oracle;

import org.flywaydb.community.database.oceanbase.OceanBaseJdbcUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

public class OceanBaseOracleModeDatabase extends Database<OceanBaseOracleModeConnection> {

    public OceanBaseOracleModeDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    protected OceanBaseOracleModeConnection doGetConnection(Connection connection) {
        return new OceanBaseOracleModeConnection(this, connection);
    }

    @Override
    public void ensureSupported(Configuration configuration) {
        ensureDatabaseIsRecentEnough("1.4");
        recommendFlywayUpgradeIfNecessary("5.0");
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String tablespace = configuration.getTablespace() == null ? "" : " TABLESPACE \"" + configuration.getTablespace() + "\"";

        return "CREATE TABLE " + table + " (\n" + "    \"installed_rank\" INT NOT NULL,\n" + "    \"version\" VARCHAR2(50),\n" + "    \"description\" VARCHAR2(200) NOT NULL,\n" + "    \"type\" VARCHAR2(20) NOT NULL,\n" + "    \"script\" VARCHAR2(1000) NOT NULL,\n" + "    \"checksum\" INT,\n" + "    \"installed_by\" VARCHAR2(100) NOT NULL,\n" + "    \"installed_on\" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,\n" + "    \"execution_time\" INT NOT NULL,\n" + "    \"success\" NUMBER(1) NOT NULL,\n" + "    CONSTRAINT \"" + table.getName() + "_pk\" PRIMARY KEY (\"installed_rank\")\n" + ")" + tablespace + ";\n" + (baseline ? getBaselineStatement(table) + ";\n" : "") + "CREATE INDEX \"" + table.getSchema().getName() + "\".\"" + table.getName() + "_s_idx\" ON " + table + " (\"success\");\n";
    }

    @Override
    public boolean supportsEmptyMigrationDescription() {
        // OceanBase Oracle mode will convert the empty string to NULL implicitly, and throw an exception as the column is NOT NULL
        return false;
    }

    @Override
    protected String doGetCurrentUser() throws SQLException {
        return getMainConnection().getJdbcTemplate().queryForString("SELECT USER FROM DUAL");
    }

    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public boolean catalogIsSchema() {
        return false;
    }

    @Override
    protected MigrationVersion determineVersion() {
        String versionNumber;
        try {
            versionNumber = OceanBaseJdbcUtils.getVersionNumber(rawMainJdbcConnection);
        } catch (SQLException e) {
            throw new FlywayException("Failed to get version number", e);
        }
        return MigrationVersion.fromVersion(versionNumber);
    }

    /**
     * Checks whether the specified query returns rows or not. Wraps the query in EXISTS() SQL function and executes it.
     * This is more preferable to opening a cursor for the original query, because a query inside EXISTS() is implicitly
     * optimized to return the first row and because the client never fetches more than 1 row despite the fetch size
     * value.
     *
     * @param query  The query to check.
     * @param params The query parameters.
     * @return {@code true} if the query returns rows, {@code false} if not.
     * @throws SQLException when the query execution failed.
     */
    boolean queryReturnsRows(String query, String... params) throws SQLException {
        return getMainConnection().getJdbcTemplate().queryForBoolean("SELECT CASE WHEN EXISTS(" + query + ") THEN 1 ELSE 0 END FROM DUAL", params);
    }

    /**
     * Checks whether the specified data dictionary view in the specified system schema is accessible (directly or
     * through a role) or not.
     *
     * @param owner the schema name, unquoted case-sensitive.
     * @param name  the data dictionary view name to check, unquoted case-sensitive.
     * @return {@code true} if it is accessible, {@code false} if not.
     * @throws SQLException if the check failed.
     */
    private boolean isDataDictViewAccessible(String owner, String name) throws SQLException {
        return queryReturnsRows("SELECT * FROM ALL_TAB_PRIVS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?" + " AND PRIVILEGE = 'SELECT'", owner, name);
    }

    /**
     * Checks whether the specified SYS view is accessible (directly or through a role) or not.
     *
     * @param name the data dictionary view name to check, unquoted case-sensitive.
     * @return {@code true} if it is accessible, {@code false} if not.
     * @throws SQLException if the check failed.
     */
    boolean isDataDictViewAccessible(String name) throws SQLException {
        return isDataDictViewAccessible("SYS", name);
    }

    /**
     * Returns the specified data dictionary view name prefixed with DBA_ or ALL_ depending on its accessibility.
     *
     * @param baseName the data dictionary view base name, unquoted case-sensitive, e.g. OBJECTS, TABLES.
     * @return the full name of the view with the proper prefix.
     * @throws SQLException if the check failed.
     */
    String dbaOrAll(String baseName) throws SQLException {
        return isDataDictViewAccessible("DBA_" + baseName) ? "DBA_" + baseName : "ALL_" + baseName;
    }


    /**
     * Checks whether XDB component is available or not.
     *
     * @return {@code true} if it is available, {@code false} if not.
     * @throws SQLException when checking availability of the component failed.
     */
    boolean isXmlDbAvailable() throws SQLException {
        return isDataDictViewAccessible("ALL_XML_TABLES");
    }

    /**
     * Returns the list of schemas that were created and are maintained by oceanbase-supplied scripts and must not be
     * changed in any other way.
     *
     * @return the set of system schema names
     */
    Set<String> getSystemSchemas() throws SQLException {

        // The list of known default system schemas
        Set<String> result = new HashSet<>(Arrays.asList("SYS", "LBACSYS", "ORAAUDITOR"));

        result.addAll(getMainConnection().getJdbcTemplate().queryForStringList("SELECT USERNAME FROM ALL_USERS WHERE REGEXP_LIKE(USERNAME, '^(APEX|FLOWS)_\\d+$') OR ORACLE_MAINTAINED = 'Y'"));

        return result;
    }
}
