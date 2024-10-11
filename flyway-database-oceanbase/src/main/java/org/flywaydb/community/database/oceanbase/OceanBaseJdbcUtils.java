/*-
 * ========================LICENSE_START=================================
 * flyway-database-oceanbase
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
/*
 * Copyright (C) Red Gate Software Ltd 2010-2024
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
package org.flywaydb.community.database.oceanbase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.flywaydb.core.internal.util.ClassUtils;
import org.flywaydb.core.internal.util.StringUtils;

public class OceanBaseJdbcUtils {

    public static String getVersionComment(Connection connection) throws SQLException {
        return queryVariable(connection, "version_comment");
    }

    public static String getVersionNumber(Connection connection) throws SQLException {
        String versionComment = getVersionComment(connection);
        if (StringUtils.hasText(versionComment)) {
            String[] parts = versionComment.split(" ");
            if (parts.length > 1) {
                return parts[1];
            }
        }
        return null;
    }

    private static String queryVariable(Connection connection, String variable) throws SQLException {
        assert StringUtils.hasText(variable);
        String sql = String.format("SHOW VARIABLES LIKE '%s'", variable);
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                return rs.getString("VALUE");
            }
        }
        return null;
    }

    public static String getCompatibleMode(Connection connection) {
        String mode;
        try (Connection conn = connection; Statement statement = conn.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW VARIABLES LIKE 'ob_compatibility_mode'");
            mode = rs.next() ? rs.getString("VALUE") : null;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get oceanbase compatible mode", e);
        }
        return mode;
    }

    public static String getDriverClass(String url, ClassLoader classLoader) {
        if (!url.startsWith("jdbc:p6spy:mysql:") && !url.startsWith("jdbc:p6spy:google:")) {
            return url.startsWith("jdbc:mysql:") ? "com.mysql.cj.jdbc.Driver" : "com.mysql.jdbc.GoogleDriver";
        } else {
            return "com.p6spy.engine.spy.P6SpyDriver";
        }
    }

    public static String getBackupDriverClass(String url, ClassLoader classLoader) {
        if (ClassUtils.isPresent("com.mysql.jdbc.Driver", classLoader)) {
            return "com.mysql.jdbc.Driver";
        } else {
            return ClassUtils.isPresent("org.mariadb.jdbc.Driver", classLoader) && !url.contains("disableMariaDbDriver") ? "org.mariadb.jdbc.Driver" : null;
        }
    }
}
