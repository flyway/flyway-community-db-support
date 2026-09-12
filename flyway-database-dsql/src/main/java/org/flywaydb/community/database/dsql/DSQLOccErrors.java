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

import java.sql.SQLException;

/**
 * Detects Aurora DSQL optimistic-concurrency-control (OCC) conflicts.
 *
 * <p>DSQL reports a conflict as SQLSTATE {@code 40001} carrying an OC code in the message —
 * {@code OC000} for data, {@code OC001} for catalog/schema — and also raises those codes as the
 * SQLSTATE itself. All three forms are matched here rather than delegated to the connector's
 * {@code OCCRetry.isOCCError}, so retry also works on {@code jdbc:postgresql://} endpoints where the
 * connector is not the driver and need not be on the classpath.
 *
 * <p>Walks the cause / next-exception chain: at the transaction-template layer the conflict arrives
 * wrapped in a {@code FlywaySqlException}, not a bare {@link SQLException}.
 */
final class DSQLOccErrors {

    private static final String OCC_DATA_CONFLICT = "OC000";
    private static final String OCC_CATALOG_CONFLICT = "OC001";
    private static final String OCC_SERIALIZATION = "40001";

    private DSQLOccErrors() {
    }

    /**
     * Returns true if the given exception, or anything in its cause / next-exception
     * chain, is an Aurora DSQL OCC conflict.
     */
    static boolean isOccError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException) {
                SQLException sqlException = (SQLException) current;
                if (isOccSqlException(sqlException)) {
                    return true;
                }
                SQLException next = sqlException.getNextException();
                if (next != null && isOccError(next)) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isOccSqlException(SQLException e) {
        String sqlState = e.getSQLState();
        if (OCC_DATA_CONFLICT.equals(sqlState) || OCC_CATALOG_CONFLICT.equals(sqlState)) {
            return true;
        }
        String message = e.getMessage();
        if (message != null
                && (message.contains(OCC_DATA_CONFLICT) || message.contains(OCC_CATALOG_CONFLICT))) {
            return true;
        }
        return OCC_SERIALIZATION.equals(sqlState);
    }
}
