package org.flywaydb.community.database.tibero;

import static org.flywaydb.community.database.tibero.FlywayForTibero.PASSWORD;
import static org.flywaydb.community.database.tibero.FlywayForTibero.SCHEMA;
import static org.flywaydb.community.database.tibero.FlywayForTibero.TIBERO_URL;
import static org.flywaydb.community.database.tibero.FlywayForTibero.USER;
import static org.flywaydb.community.database.tibero.FlywayForTibero.createFlyway;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TiberoFlywayRepairTest {

	@BeforeEach
	@AfterEach
	void cleanup() throws SQLException {
		try (Connection connection = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
			 Statement statement = connection.createStatement();
			 ResultSet resultSet = statement.executeQuery(
				 "SELECT TABLE_NAME FROM USER_TABLES WHERE TABLESPACE_NAME = " + "'" + SCHEMA + "'")) {

			List<String> tables = new ArrayList<>();
			while (resultSet.next()) {
				tables.add(resultSet.getString("TABLE_NAME"));
			}

			for (String tableName : tables) {
				String dropTableQuery = "DROP TABLE \"" + tableName + "\" CASCADE CONSTRAINTS";
				statement.executeUpdate(dropTableQuery);
			}
		}
	}

	@Test
	@DisplayName("Repair test")
	void repairFailedMigration() throws SQLException {

		SoftAssertions softAssertions = new SoftAssertions();

		// 1. Attempt to run a failing migration
		Flyway failedFlyway = createFlyway("classpath:db/migration-with-failed");
		softAssertions.assertThatThrownBy(failedFlyway::migrate).isInstanceOf(FlywayException.class);

		// 2. Verify the failed migration
		try (Connection conn = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery(
				 "SELECT COUNT(*) FROM TIBERO.\"flyway_schema_history\" WHERE \"success\" = 0")) {

			rs.next();

			softAssertions.assertThat(rs.getInt(1)).isEqualTo(1);
		}

		// 3. Execute Repair
		failedFlyway.repair();

		// 4. Verify that the failed migration has been removed after Repair
		try (Connection conn = DriverManager.getConnection(TIBERO_URL, USER, PASSWORD);
			 Statement stmt = conn.createStatement();
			 ResultSet rs = stmt.executeQuery(
				 "SELECT COUNT(*) FROM TIBERO.\"flyway_schema_history\" WHERE \"success\" = 0")) {
			rs.next();
			softAssertions.assertThat(rs.getInt(1)).isEqualTo(1);
		}

		softAssertions.assertAll();
	}

}
