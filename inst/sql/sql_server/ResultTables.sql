CREATE TABLE @my_schema.@table_prefixtime_to_event (
    database_id varchar(100) NOT NULL,
    target_cohort_definition_id bigint NOT NULL,
    outcome_cohort_definition_id bigint NOT NULL,
    outcome_type varchar(20) NOT NULL,
    target_outcome_type varchar(40) NOT NULL,
    time_to_event int NOT NULL,
    num_events int NOT NULL,
    time_scale varchar(20) NOT NULL,
    PRIMARY KEY (database_id, target_cohort_definition_id, outcome_cohort_definition_id, outcome_type, target_outcome_type, time_to_event, time_scale)
);

CREATE TABLE @my_schema.@table_prefixrechallenge_fail_case_series (
    --run_id,
    database_id varchar(100) NOT NULL,
    dechallenge_stop_interval int NOT NULL,
    dechallenge_evaluation_window int NOT NULL,
    target_cohort_definition_id bigint NOT NULL,
    outcome_cohort_definition_id bigint NOT NULL,
    person_key int NOT NULL,
    subject_id bigint,
    dechallenge_exposure_number int NOT NULL,
    dechallenge_exposure_start_date_offset int NOT NULL,
    dechallenge_exposure_end_date_offset int NOT NULL,
    dechallenge_outcome_number int NOT NULL,
    dechallenge_outcome_start_date_offset int NOT NULL,
    rechallenge_exposure_number int NOT NULL,
    rechallenge_exposure_start_date_offset int NOT NULL,
    rechallenge_exposure_end_date_offset int NOT NULL,
    rechallenge_outcome_number int NOT NULL,
    rechallenge_outcome_start_date_offset int NOT NULL,
    PRIMARY KEY (database_id, dechallenge_stop_interval,dechallenge_evaluation_window, target_cohort_definition_id,
    outcome_cohort_definition_id, person_key, dechallenge_exposure_number, dechallenge_outcome_number,
    rechallenge_exposure_number, rechallenge_outcome_number)
);

CREATE TABLE @my_schema.@table_prefixdechallenge_rechallenge (
    database_id varchar(100) NOT NULL,
    dechallenge_stop_interval int NOT NULL,
    dechallenge_evaluation_window int NOT NULL,
    target_cohort_definition_id bigint NOT NULL,
    outcome_cohort_definition_id bigint NOT NULL,
    num_exposure_eras int NOT NULL,
    num_persons_exposed int NOT NULL,
    num_cases int,
    dechallenge_attempt int,
    dechallenge_fail int,
    dechallenge_success int,
    rechallenge_attempt int,
    rechallenge_fail int,
    rechallenge_success int,
    pct_dechallenge_attempt float,
    pct_dechallenge_success float,
    pct_dechallenge_fail float,
    pct_rechallenge_attempt float,
    pct_rechallenge_success float,
    pct_rechallenge_fail float,
    PRIMARY KEY (database_id, dechallenge_stop_interval, dechallenge_evaluation_window, target_cohort_definition_id,
    outcome_cohort_definition_id)
);


-- covariateSettings
CREATE TABLE @my_schema.@table_prefixsettings (
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    covariate_setting_json varchar(MAX),
    risk_window_start int,
    start_anchor varchar(15),
    risk_window_end int,
    end_anchor varchar(15),
    PRIMARY KEY (run_id, database_id)
);

-- added this table
CREATE TABLE @my_schema.@table_prefixcohort_details (
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    cohort_definition_id bigint NOT NULL,
    target_cohort_id int,
    outcome_cohort_id int,
    cohort_type varchar(10),
    PRIMARY KEY (run_id, database_id,cohort_definition_id)
);

CREATE TABLE @my_schema.@table_prefixanalysis_ref (
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    analysis_id int NOT NULL,
    analysis_name varchar(max) NOT NULL,
    domain_id varchar(30) NOT NULL,
    start_day int,
    end_day int,
    is_binary varchar(1),
    missing_means_zero varchar(1),
    PRIMARY KEY (database_id, run_id, analysis_id )
);

CREATE TABLE @my_schema.@table_prefixcovariate_ref (
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    covariate_id bigint NOT NULL,
    covariate_name varchar(max) NOT NULL,
    analysis_id int NOT NULL,
    concept_id bigint,
    PRIMARY KEY (database_id, run_id, covariate_id)
);

CREATE TABLE @my_schema.@table_prefixcovariates (
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    cohort_definition_id bigint NOT NULL,
    covariate_id bigint NOT NULL,
    sum_value int NOT NULL,
    average_value float NOT NULL,
    PRIMARY KEY (database_id, run_id, cohort_definition_id, covariate_id)
);

CREATE TABLE @my_schema.@table_prefixcovariates_continuous (
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    cohort_definition_id bigint NOT NULL,
    covariate_id bigint NOT NULL,
    count_value int NOT NULL,
    min_value float,
    max_value float,
    average_value float,
    standard_deviation float,
    median_value float,
    p_10_value float,
    p_25_value float,
    p_75_value float,
    p_90_value float,
    PRIMARY KEY (database_id, run_id, cohort_definition_id, covariate_id)
);

CREATE TABLE @my_schema.@table_prefixcohort_counts(
    run_id int NOT NULL,
    database_id varchar(100) NOT NULL,
    cohort_definition_id bigint NOT NULL,
    row_count int NOT NULL,
    person_count int NOT NULL,
    PRIMARY KEY (run_id, database_id, cohort_definition_id)
);
