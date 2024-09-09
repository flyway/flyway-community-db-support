package org.flywaydb.community.database.tibero;

import org.flywaydb.core.Flyway;

class FlywayForTibero extends TestContainerBaseTests {

    public static final String TIBERO_URL = getJdbcUrl();
    public static final String SCHEMA = "TIBERO";
    public static final String USER = getUsername();
    public static final String PASSWORD = getPassword();

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
