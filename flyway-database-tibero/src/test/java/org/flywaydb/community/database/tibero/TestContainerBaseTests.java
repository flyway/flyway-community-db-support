package org.flywaydb.community.database.tibero;

import java.io.File;
import java.time.Duration;

import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

abstract class TestContainerBaseTests {

	@Container
	public static DockerComposeContainer<?> environment =
		new DockerComposeContainer<>(new File("src/test/resources/docker/docker-compose.yml"))
			.withExposedService("tibero-test", 8629,
				Wait.forLogMessage(".*database system is ready to accept connections.*", 1)
					.withStartupTimeout(Duration.ofMinutes(5)));

	static {
		environment.start();
	}

	protected static String getJdbcUrl() {
		return String.format("jdbc:tibero:thin:@%s:%d:tibero",
			environment.getServiceHost("tibero-test", 8629),
			environment.getServicePort("tibero-test", 8629));
	}

	protected static String getUsername() {
		return "tibero";
	}

	protected static String getPassword() {
		return "tibero";
	}
}


