DROP TABLE IF EXISTS @characterization_schema.@characterization_table;

CREATE TABLE @characterization_schema.@characterization_table(
cohort_definition_id BIGINT,
row_number BIGINT,
subject_id BIGINT,
cohort_start_date DATE,
cohort_end_date DATE,
observation_period_start_date DATE,
observation_period_end_date DATE,
char_type VARCHAR(20)
);

DROP TABLE IF EXISTS @characterization_schema.@attrition_table;
CREATE TABLE @characterization_schema.@attrition_table(
cohort_definition_id BIGINT,
attr_reason VARCHAR(50),
n BIGINT
);
