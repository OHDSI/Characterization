DROP TABLE IF EXISTS @characterization_schema.@characterization_table;

CREATE TABLE @characterization_schema.@characterization_table(
cohort_definition_id BIGINT,
row_id BIGINT,
subject_id BIGINT,
cohort_start_date DATE,
cohort_end_date DATE,
observation_period_start_date DATE,
observation_period_end_date DATE,
char_type VARCHAR(20)
);

DROP TABLE IF EXISTS @characterization_schema.@target_attrition_table;
CREATE TABLE @characterization_schema.@target_attrition_table(
characterization_target_id BIGINT,
attr_order INT,
attr_reason VARCHAR(200),
n_events BIGINT,
n_people BIGINT
);

DROP TABLE IF EXISTS @characterization_schema.@case_attrition_table;
CREATE TABLE @characterization_schema.@case_attrition_table(
characterization_case_id BIGINT,
attr_order INT,
attr_reason VARCHAR(200),
n_events BIGINT,
n_people BIGINT
);

-- count tables
DROP TABLE IF EXISTS @characterization_schema.@target_count_table;
CREATE TABLE @characterization_schema.@target_count_table(
characterization_target_id BIGINT,
n_events BIGINT,
n_people BIGINT
);

DROP TABLE IF EXISTS @characterization_schema.@case_count_table;
CREATE TABLE @characterization_schema.@case_count_table(
characterization_case_id BIGINT,
cohort_type VARCHAR(10),
n_events BIGINT,
n_people BIGINT
);

-- outcome era table
DROP TABLE IF EXISTS @characterization_schema.@outcome_era_table;
CREATE TABLE @characterization_schema.@outcome_era_table(
cohort_definition_id BIGINT,
outcome_washout BIGINT,
subject_id BIGINT,
cohort_start_date DATE,
cohort_end_date DATE
);
