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

import lombok.CustomLog;

import org.flywaydb.core.api.callback.Callback;
import org.flywaydb.core.api.callback.Context;
import org.flywaydb.core.api.callback.Event;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.JdbcUtils;

import java.sql.PreparedStatement;
import java.util.stream.IntStream;

@CustomLog
public class IRISCallback implements Callback {

    @Override
    public boolean supports(Event event, Context context) {
        return true;
    }

    @Override
    public boolean canHandleInTransaction(Event event, Context context) {
        return false;
    }

    @Override
    public void handle(Event event, Context context) {
        if (event.equals(Event.AFTER_MIGRATE)) {
            IntStream.range(0, IRISTable.totalLocks.get()).forEach(i -> {
                unlock(IRISTable.lockedJdbcTemplate);
            });
            IRISTable.totalLocks.set(0);
        }
        IRISTable.lockedJdbcTemplate = null;
        IRISTable.lockedTable = null;
    }

    @Override
    public String getCallbackName() {
        return this.getClass().getSimpleName();
    }

    private void unlock(JdbcTemplate jdbcTemplate) {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = jdbcTemplate.getConnection().prepareStatement("UNLOCK " + IRISTable.lockedTable + " IN EXCLUSIVE MODE");
            preparedStatement.execute();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            JdbcUtils.closeStatement(preparedStatement);
        }
    }
}
