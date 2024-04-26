package org.flywaydb.community.database;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.extensibility.PluginMetadata;
import org.flywaydb.core.internal.util.FileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CrateDatabaseExtension implements PluginMetadata {
    public String getDescription() {
        return "Community-contributed Crate database support extension " + readVersion() + " by JaVol";
    }

    public static String readVersion() {
        try {
            return FileUtils.copyToString(
                    CrateDatabaseExtension.class.getClassLoader().getResourceAsStream("org/flywaydb/community/database/crate/version.txt"),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FlywayException("Unable to read extension version: " + e.getMessage(), e);
        }
    }
}
