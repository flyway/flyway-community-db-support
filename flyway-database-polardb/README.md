# Flyway PolarDB database plugin

This module adds Flyway database detection and SQL statement parsing for
PolarDB for PostgreSQL in both PostgreSQL and Oracle compatibility modes.

The implementation reuses Flyway's PostgreSQL support for schemas, tables,
transactions, advisory locks, and schema history. In Oracle compatibility
mode, it extends the PostgreSQL parser to support PolarDB PL/SQL constructs,
including:

- standalone procedures and functions
- anonymous `DECLARE` and `BEGIN` blocks
- package specifications and package bodies
- triggers

PostgreSQL dollar-quoted functions, procedures, and blocks remain supported
in PostgreSQL compatibility mode.

## Requirements

- A Flyway installation containing this plugin
- The Java version required by the Flyway release in use. This repository is
  currently built and tested with Java 17.
- A JDBC driver appropriate for the PolarDB compatibility mode

Flyway does not normally bundle JDBC drivers for community-contributed
databases. Install the required JDBC driver in the Flyway `jars` directory.

### PostgreSQL compatibility mode

Use the community PostgreSQL JDBC driver with a PostgreSQL JDBC URL:

```text
jdbc:postgresql://host:5432/database
```

### Oracle compatibility mode

Use the PolarDB JDBC driver. The driver class is:

```text
com.aliyun.polardb2.Driver
```

PolarDB JDBC 42.5.7.0.15 and later is available from Maven Central:

```xml
<dependency>
    <groupId>com.aliyun</groupId>
    <artifactId>polardb</artifactId>
    <version>42.5.7.0.15</version>
</dependency>
```

See the
[PolarDB JDBC documentation](https://www.alibabacloud.com/help/en/polardb/polardb-for-oracle/jdbc-o-2-0)
for driver downloads, connection properties, and current versions.

## JDBC URL and driver selection

The JDBC driver is selected before a database connection is available, so
the plugin selects it from the JDBC URL:

| JDBC URL | Driver |
| --- | --- |
| `jdbc:postgresql://host:5432/database` | `org.postgresql.Driver` |
| `jdbc:polardb://host:1521/database` | `com.aliyun.polardb2.Driver` |
| `jdbc:postgresql://host:1521/database?forceDriverType=true` | `com.aliyun.polardb2.Driver` |

The native PolarDB URL is recommended when using the PolarDB driver:

```text
jdbc:polardb://host:1521/database
```

Alternatively, append `forceDriverType=true` to a PostgreSQL URL to
explicitly select the PolarDB driver:

```text
jdbc:postgresql://host:1521/database?forceDriverType=true
```

## Compatibility mode detection

After establishing a connection, the plugin reads the database setting:

```sql
SELECT current_setting('polar_compatibility_mode', true)
```

The `pg` and `ora` values are handled by the same PolarDB database type. A
missing, unknown, or unreadable setting is not treated as PolarDB. This
prevents the plugin from claiming regular PostgreSQL or another
PostgreSQL-compatible database.

PostgreSQL mode uses nested block comments. Oracle mode uses non-nested block
comments, matching the corresponding PolarDB SQL dialect behavior.

## Build and install from source

Build the plugin from the repository root:

```shell
bash ./mvnw -pl flyway-database-polardb -am package
```

Copy the plugin and the required JDBC driver to the `jars` directory of the
Flyway installation:

```shell
cp flyway-database-polardb/target/flyway-database-polardb-*.jar \
   "$FLYWAY_HOME/jars/"
cp /path/to/polardb-jdbc.jar "$FLYWAY_HOME/jars/"
```

For a Homebrew Flyway installation, prefer a project-local Flyway
distribution or an installation script because Homebrew upgrades replace
files inside the Cellar.

## Flyway configuration example

```toml
[environments.polardb]
url = "jdbc:polardb://host:1521/database"
user = "database_user"
password = "database_password"
schemas = ["application_schema"]

[flyway]
environment = "polardb"
locations = ["filesystem:sql"]
```

Run Flyway from the directory containing `flyway.toml`:

```shell
flyway info
flyway migrate
flyway validate
```
