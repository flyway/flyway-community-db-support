-- create view
CREATE TABLE CONTRACT
(age NUMBER, sex NUMBER, salary NUMBER,
 workplace VARCHAR(30));

CREATE VIEW CONTRACT_VIEW AS
    SELECT age, sex, salary * 1000 salary_won
    FROM contract
    WHERE workplace = 'Seoul';

-- create index
CREATE INDEX MY_USERS_NAME_INDEX ON MY_USERS (NAME);

-- create sequence
CREATE SEQUENCE "TEMP_SEQ" INCREMENT BY 1 START WITH 1 ORDER;

-- crate materialized view
CREATE TABLE MV_TABLE AS (
    SELECT MOD(level, 100) a, level*10 b FROM dual CONNECT BY level<=100);

CREATE MATERIALIZED VIEW MV ENABLE QUERY rewrite AS
    SELECT SUM(a+b) s, COUNT(b+a) c FROM MV_TABLE;

-- create queue table
CALL DBMS_AQADM.CREATE_QUEUE_TABLE('MY_QUEUE_TABLE', 'RAW');

-- -- create scheduler Program
-- CALL DBMS_SCHEDULER.CREATE_PROGRAM(program_name   => 'MY_PROGRAM',
--                                   program_type   => 'PSM_BLOCK',
--                                   program_action => 'my_job;');
--
-- -- create schedule
-- CALL DBMS_SCHEDULER.CREATE_SCHEDULE(
--         schedule_name   => 'MY_SCHEDULE',
--         repeat_interval => 'FREQ=HOURLY; BYHOUR=1,2,3; BYMINUTE=0; BYSECOND=30');
--
-- -- create scheduler Chain
-- CALL DBMS_SCHEDULER.CREATE_CHAIN(chain_name          => 'MY_SCHEDULE_CHAIN',
--                                 rule_set_name       => NULL,
--                                 evaluation_interval => NULL,
--                                 comments            => 'my first job chain');
--
-- -- create scheduler Chain Rule
-- CALL DBMS_SCHEDULER.DEFINE_CHAIN_RULE (
--                                   'MY_SCHEDULE_CHAIN_RULE',
--                                   '1=1',
--                                   'start step1',
--                                   'RULE1');
--
-- -- create scheduler Chain Step
-- CALL DBMS_SCHEDULER.DEFINE_CHAIN_STEP (
--                                   'MY_SCHEDULE_CHAIN_STEP',
--                                   'STEP1',
--                                   'example_program');

-- create synonym
CREATE OR REPLACE SYNONYM my_synonym FOR tibero.emp;