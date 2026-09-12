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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the schema-history reconcile statement. Only the statement is unit-testable:
 * {@code PostgreSQLConnection}'s constructor queries {@code CURRENT_USER}, so a DSQLConnection
 * instance cannot be built without a live cluster (exercised by {@link DSQLIntegrationTest}).
 */
class DSQLConnectionTest {

    private static final String TABLE = "\"public\".\"flyway_schema_history\"";

    @Test
    void reconcileStatementDeletesOnlyLowerRankedSuccessfulRowsOfTheSameVersionAndType() {
        assertThat(DSQLConnection.reconcileStatement(TABLE)).isEqualTo(
                "DELETE FROM \"public\".\"flyway_schema_history\" h"
                        + " WHERE h.\"version\" IS NOT NULL"
                        + " AND h.\"success\" = TRUE"
                        + " AND h.\"installed_rank\" < ("
                        + "SELECT MAX(h2.\"installed_rank\") FROM \"public\".\"flyway_schema_history\" h2"
                        + " WHERE h2.\"version\" = h.\"version\""
                        + " AND h2.\"type\" = h.\"type\""
                        + " AND h2.\"success\" = TRUE)");
    }

    /**
     * Matching on version alone would delete the row a repair {@code DELETE} marker or an undo entry
     * refers to, since Flyway appends those above it.
     */
    @Test
    void reconcileStatementComparesTypeInBothOuterRowAndSubquery() {
        assertThat(DSQLConnection.reconcileStatement(TABLE))
                .contains("h2.\"type\" = h.\"type\"");
    }

    /**
     * A migration that exhausts its retries records a failed row above the successful ones (DSQL has
     * no DDL transactions, so failures are recorded). Restricting the subquery to successful rows
     * keeps that row from making every successful row below it look superseded.
     */
    @Test
    void reconcileStatementRestrictsBothSidesToSuccessfulRows() {
        assertThat(DSQLConnection.reconcileStatement(TABLE))
                .containsSubsequence("h.\"success\" = TRUE", "h2.\"success\" = TRUE");
    }
}
