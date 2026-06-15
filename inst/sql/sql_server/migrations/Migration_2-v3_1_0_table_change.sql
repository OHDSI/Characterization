-- Database migrations for verion 0.3.0
-- This migration updates the schema:
 -- 1. to store the charcterization version
 -- 2. Add a migrations table for supporting database migrations

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}



-- ===========================
-- 1) Create target_attrition table
-- ===========================
DROP TABLE IF EXISTS @database_schema.@table_prefixtarget_attrition;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefixtarget_attrition(
  characterization_target_id BIGINT,
  attr_order	INT,
  attr_reason	VARCHAR(100),
  n_events	BIGINT,
  n_people	BIGINT,
  database_id	VARCHAR(100),
  setting_id VARCHAR(50),
  PRIMARY KEY (setting_id, database_id, characterization_target_id, attr_order)
);
-- ===========================


-- ===========================
-- 2) Create case_attrition table
-- ===========================
DROP TABLE IF EXISTS @database_schema.@table_prefixcase_attrition;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefixcase_attrition(
  characterization_case_id BIGINT,
  attr_order	INT,
  attr_reason	VARCHAR(100),
  n_events	BIGINT,
  n_people	BIGINT,
  database_id	VARCHAR(100),
  setting_id VARCHAR(50),
  PRIMARY KEY (setting_id, database_id, characterization_case_id, attr_order)
);
-- ===========================




-- ===========================
-- 3) Create target_count table
-- ===========================
DROP TABLE IF EXISTS @database_schema.@table_prefixtarget_counts;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefixtarget_counts(
  characterization_target_id BIGINT,
  n_events BIGINT,
  n_people BIGINT,
  database_id VARCHAR(100),
  setting_id VARCHAR(50),
  PRIMARY KEY (setting_id, database_id, characterization_target_id)
);
-- ===========================


-- ===========================
-- 4) Create case_count table
-- ===========================
DROP TABLE IF EXISTS @database_schema.@table_prefixcase_counts;

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefixcase_counts(
  characterization_case_id BIGINT,
  cohort_type VARCHAR(50),
  n_events BIGINT,
  n_people BIGINT,
  database_id VARCHAR(100),
  setting_id VARCHAR(50),
  PRIMARY KEY (setting_id, database_id, characterization_case_id, cohort_type)
);
-- ===========================


-- ===========================
-- 5) Rename target_cohort_definition_id to characterization_target_id
-- ===========================
-- dechallenge_rechallenge/rechallenge_fail_case_series/time_to_event
-- Change to target_cohort_definition_id characterization_target_id

ALTER TABLE  @database_schema.@table_prefixdechallenge_rechallenge
RENAME COLUMN target_cohort_definition_id to characterization_target_id;

ALTER TABLE  @database_schema.@table_prefixrechallenge_fail_case_series
RENAME COLUMN target_cohort_definition_id to characterization_target_id;

ALTER TABLE  @database_schema.@table_prefixtime_to_event
RENAME COLUMN target_cohort_definition_id to characterization_target_id;


-- ===========================
-- 6) Add columns in target_settings
-- ===========================
-- target_settings: add
--    nesting_cohort_id bigint / min_age int / max_age int
--    study_start date / study_end date / gender_concept_ids varchar(100)
--    time_to_event_settings bit / dechallenge_rechallenge_settings bit
--    target_baseline_settings bit / risk_factor_settings bit / case_series_settings bit

ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN nesting_cohort_id BIGINT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN min_age INT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN max_age INT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN study_start DATE;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN study_end DATE;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN gender_concept_ids VARCHAR(100);
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN time_to_event_settings BIT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN dechallenge_rechallenge_settings BIT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN target_baseline_settings BIT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN risk_factor_settings BIT;
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN case_series_settings BIT;


-- ===========================
-- 7) Add/remove columns in case_settings
-- ===========================
-- case_settings:
--   remove: runtype
ALTER TABLE @database_schema.@table_prefixcase_settings DROP COLUMN runtype;
--   add: risk_factor_settings varchar(50) / case_series_settings varchar(50)
ALTER TABLE  @database_schema.@table_prefixcase_settings
ADD COLUMN risk_factor_settings VARCHAR(50);
ALTER TABLE  @database_schema.@table_prefixcase_settings
ADD COLUMN case_series_settings VARCHAR(50);

