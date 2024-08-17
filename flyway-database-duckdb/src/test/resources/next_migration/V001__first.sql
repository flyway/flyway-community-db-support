CREATE TABLE ${flyway:defaultSchema}.some_table(
    id INT,
    text VARCHAR
);

CREATE INDEX some_table_text ON ${flyway:defaultSchema}.some_table(text);

CREATE SEQUENCE some_sequence;

INSERT INTO ${flyway:defaultSchema}.some_table(id, text) VALUES (1, 'first');
INSERT INTO ${flyway:defaultSchema}.some_table(id, text) VALUES (2, 'second');