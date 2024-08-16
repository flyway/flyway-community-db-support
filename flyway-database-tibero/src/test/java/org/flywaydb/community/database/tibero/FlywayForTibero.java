package org.flywaydb.community.database.tibero;

import org.flywaydb.core.Flyway;

public class FlywayForTibero {

    public static final String TIBERO_URL = "jdbc:tibero:thin:@localhost:8629:tibero";
    public static final String USER = "tibero";
    public static final String PASSWORD = "tibero";

    /**
     * Create a Flyway instance for Tibero cleanDisabled is false
     * @param locations
     * @return
     */
    public static Flyway createFlyway(String locations) {
        return createFlyway(locations, false);
    }

    /**
     * Create a Flyway instance for Tibero
     * @param locations
     * @param cleanDisabled
     * @return
     */
    public static Flyway createFlyway(String locations, boolean cleanDisabled) {

        return Flyway.configure()
            .locations(locations)
            .cleanDisabled(cleanDisabled)
            .dataSource(TIBERO_URL, USER, PASSWORD)
            .load();
    }
}
