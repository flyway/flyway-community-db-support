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

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class DSQLOccErrorsTest {

    @Test
    void detectsOccSqlStates() {
        assertThat(DSQLOccErrors.isOccError(new SQLException("data conflict", "OC000"))).isTrue();
        assertThat(DSQLOccErrors.isOccError(new SQLException("catalog conflict", "OC001"))).isTrue();
        assertThat(DSQLOccErrors.isOccError(new SQLException("serialization", "40001"))).isTrue();
    }

    @Test
    void detectsOccCodeCarriedInMessage() {
        // DSQL reports a catalog conflict as SQLSTATE 40001 with the OC code only in the message.
        assertThat(DSQLOccErrors.isOccError(
                new SQLException("schema has been updated by another transaction (OC001)", "40001"))).isTrue();
        assertThat(DSQLOccErrors.isOccError(
                new SQLException("write conflict (OC000)", "XX000"))).isTrue();
    }

    @Test
    void ignoresNonOccSqlStates() {
        assertThat(DSQLOccErrors.isOccError(new SQLException("syntax error", "42601"))).isFalse();
        assertThat(DSQLOccErrors.isOccError(new SQLException("no state", (String) null))).isFalse();
    }

    @Test
    void detectsOccInCauseChain() {
        SQLException root = new SQLException("catalog conflict", "OC001");
        SQLException wrapper = new SQLException("wrapped", "XX000");
        wrapper.initCause(root);
        assertThat(DSQLOccErrors.isOccError(wrapper)).isTrue();
    }

    @Test
    void detectsOccInNextExceptionChain() {
        SQLException first = new SQLException("first", "XX000");
        SQLException next = new SQLException("serialization", "40001");
        first.setNextException(next);
        assertThat(DSQLOccErrors.isOccError(first)).isTrue();
    }

    @Test
    void nullIsNotOcc() {
        assertThat(DSQLOccErrors.isOccError(null)).isFalse();
    }
}
