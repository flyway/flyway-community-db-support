package org.flywaydb.community.database;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.extensibility.PluginMetadata;
import org.flywaydb.core.internal.util.FileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class TiberoDatabaseExtension implements PluginMetadata {

    public String getDescription() {
        return "Community-contributed Tibero database support extension " + readVersion() + " by Redgate";
    }

    public static String readVersion() {
        try {
            return FileUtils.copyToString(
                    Objects.requireNonNull(TiberoDatabaseExtension.class.getClassLoader()
                            .getResourceAsStream("org/flywaydb/community/database/tibero/version.txt")),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FlywayException("Unable to read extension version: " + e.getMessage(), e);
        }
    }
}
