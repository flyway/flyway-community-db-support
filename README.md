# Flyway for Tibero

## Table of Contents
1. [Introduction](#introduction)
2. [License](#license)
3. [Key Modifications](#key-modifications)
4. [Getting Started](#getting-started)
    - [Adding Dependencies](#adding-dependencies)
        - [Gradle](#gradle)
        - [Maven](#maven)
5. [Basic Usage](#basic-usage)
    - [Clean](#clean)
    - [Baseline](#baseline)
    - [Migrate](#migrate)
6. [Version Support](#version-support)
7. [Contributing](#contributing)
8. [Support](#support)

## Introduction

Flyway for Tibero is an extension of the Flyway project, specifically designed to support Tibero databases. This project is a fork of [flyway-community-db-support](https://github.com/flyway/flyway-community-db-support), tailored to meet the needs of Tibero users. Flyway is a powerful open-source database migration tool, and this extension allows Tibero users to leverage its robust features for efficient database schema management.

## License

This project is distributed under the Apache License 2.0, maintaining consistency with the original Flyway project. The full text of the license can be found in the [LICENSE](LICENSE) file. All contributions to this project are subject to the same license terms.

## Key Modifications

The following key modifications have been made to the original Flyway project:

- Added support for Tibero databases
- Implemented Tibero-specific SQL syntax handling
- Optimized Tibero connection and transaction management
- Adjusted Tibero metadata table structure

## Getting Started

### Adding Dependencies

#### Gradle

Add the following to your `build.gradle` file:

```groovy
repositories {
    maven {
        url "${github repo url}"
    }
}

dependencies {
    implementation "org.flywaydb:flyway-core:${flywayVersion}"
    
    // implementation tibero jar
    // implemetation flyway for tibero
}
```

#### Maven

```xml
<repositories>
   <repository>
      <id>github</id>
      <url>${github repo url}</url>
   </repository>
</repositories>

<dependencies>
   <dependency>
      <groupId>org.flywaydb</groupId>
      <artifactId>flyway-core</artifactId>
      <version>${flywayVersion}</version>
   </dependency>
   <dependency>
      <!--tibero jar-->
   </dependency>
</dependencies>
```
## Basic Usage

Flyway for Tibero provides several core commands to manage your database migrations. Here's a detailed explanation of each command and how to use them:

### Clean

The `clean` command drops all objects in the configured schemas, effectively wiping the database clean.

```java
Flyway flyway = Flyway.configure()
	.cleanDisabled(false)
	.dataSource(url, user, password)
	.load();

flyway.clean();
```

**When to use:** Use this command with caution, typically in development or testing environments when you want to start fresh. Never use it in production without careful consideration.

**Example scenario:** You've been testing various migrations and want to reset your database to start over.

**Important notes:**
- To enable the clean command, you must set the flyway.cleanDisabled property to false in your configuration.
- The clean command may not work on certain schemas, particularly those owned by the database system. Always test this command in a safe environment before using it.

### Baseline

The `baseline` command marks an existing database state as the starting point for further migrations. It's useful when you're introducing Flyway to an existing project.

```java
Flyway flyway = Flyway.configure()
	.locations("classpath:db/migration", "filesystem:/path/to/migrations")
	.dataSource(url, user, password)
	.load();

flyway.baseline();
```

**When to use:** When you have an existing database and want to start using Flyway to manage future changes.

**Example scenario:** You have a database in production and want to start using Flyway for future updates without affecting existing data.

### Migrate

The `migrate` command is the core functionality of Flyway. It scans for available migrations and applies any that haven't been run yet.

```java
Flyway flyway = Flyway.configure()
	.locations("classpath:db/migration", "filesystem:/path/to/migrations")
	.dataSource("${url}", "${username}", "${password}")
	.load();

flyway.migrate();
```

**When to use:** Regularly, as part of your deployment process to keep your database schema up-to-date.

**Example scenario:** You've added a new migration script to create a table. Running migrate will apply this change to your database.

**Important notes:**
- The `migrate` command can be used on both empty and existing databases. For existing databases, it's recommended to use the baseline command first if the current state doesn't match your initial migration script.
- Flyway uses the `locations` parameter to find migration scripts. This can be configured as shown above.

### Other Commands and Features

Flyway offers several other useful commands and features, such as `info`, `validate`, `repair`, and more. For a comprehensive guide on these additional functionalities, we kindly recommend referring to the official Flyway documentation:

[Flyway Official Documentation](https://documentation.red-gate.com/flyway)

This resource provides detailed explanations and examples for all Flyway commands and features, ensuring you have access to the most up-to-date and accurate information.

## Version Support
Currently, Flyway for Tibero is designed and tested to support `Tibero 7`. Support for other versions of Tibero is planned for future releases. We are actively working on expanding our version compatibility to ensure broader coverage for Tibero users.

## Contributing
We welcome contributions to this project. Whether it's bug reports, feature suggestions, or pull requests, your input is valuable. Please note that all contributions must adhere to the Apache License 2.0 terms.

## Support
If you encounter any issues or have questions, please create a GitHub issue or contact the project maintainers directly.