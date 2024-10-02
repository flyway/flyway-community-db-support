CREATE MACRO ${flyway:defaultSchema}.some_is_empty(a) AS a == '';

CREATE VIEW ${flyway:defaultSchema}.some_view AS
SELECT * FROM ${flyway:defaultSchema}.some_table WHERE NOT some_is_empty(text);