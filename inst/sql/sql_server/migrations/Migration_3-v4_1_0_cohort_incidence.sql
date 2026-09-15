{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}



-- ===========================
-- 1) Create incidence_summary table
-- ===========================
--HINT DISTRIBUTE ON RANDOM
CREATE TABLE @database_schema.@table_prefixincidence_summary(
  setting_id varchar(50),
  ref_id INT,
  database_id VARCHAR(255),
  source_name VARCHAR(255),
  -- target_cohort_definition_id BIGINT,
  characterization_target_id BIGINT,
  tar_id BIGINT,
  subgroup_id BIGINT,
  outcome_id BIGINT,
  age_group_id INT,
  gender_id INT,
  gender_name VARCHAR(255),
  start_year INT,
  persons_at_risk_pe BIGINT,
  persons_at_risk BIGINT,
  person_days_pe BIGINT,
  person_days BIGINT,
  person_outcomes_pe BIGINT,
  person_outcomes BIGINT,
  outcomes_pe BIGINT,
  outcomes BIGINT,
  incidence_proportion_p_100p FLOAT,
  incidence_rate_p_100py FLOAT
  --PRIMARY KEY (ref_id, database_id, characterization_target_id, tar_id, subgroup_id, outcome_id,
  -- age_group_id, gender_id, start_year)
);
-- ===========================


CREATE TABLE @database_schema.@table_prefixtarget_def(
  setting_id varchar(50),
  ref_id INT,
  --target_cohort_definition_id BIGINT,
  characterization_target_id BIGINT,
  target_name VARCHAR(255),
  PRIMARY KEY (ref_id, characterization_target_id)
);

CREATE TABLE @database_schema.@table_prefixoutcome_def(
  setting_id varchar(50),
  ref_id INT,
  outcome_id BIGINT,
  outcome_cohort_definition_id BIGINT,
  outcome_name VARCHAR(255),
  clean_window BIGINT,
  excluded_cohort_definition_id BIGINT,
  PRIMARY KEY (ref_id, outcome_id)
);

CREATE TABLE @database_schema.@table_prefixtar_def(
  setting_id varchar(50),
  ref_id INT,
  tar_id BIGINT,
  tar_start_with VARCHAR(10),
  tar_start_offset BIGINT,
  tar_end_with VARCHAR(10),
  tar_end_offset BIGINT,
  PRIMARY KEY (ref_id, tar_id)
);

CREATE TABLE @database_schema.@table_prefixage_group_def(
  setting_id varchar(50),
  ref_id INT,
  age_group_id INT,
  age_group_name VARCHAR(255),
  min_age INT,
  max_age INT,
  PRIMARY KEY (ref_id, age_group_id)
);

CREATE TABLE @database_schema.@table_prefixsubgroup_def(
  setting_id varchar(50),
  ref_id INT,
  subgroup_id BIGINT,
  subgroup_name VARCHAR(255),
  PRIMARY KEY (ref_id, subgroup_id)
);

CREATE TABLE @database_schema.@table_prefixtarget_outcome_ref(
  setting_id varchar(50),
  ref_id INT,
  --target_cohort_id BIGINT,
  characterization_target_id BIGINT,
  outcome_cohort_id BIGINT,
  PRIMARY KEY (ref_id, characterization_target_id, outcome_cohort_id)
);


-- add cohort_incidence_settings to target_settings
ALTER TABLE  @database_schema.@table_prefixtarget_settings
ADD COLUMN cohort_incidence_settings CHAR(1);
