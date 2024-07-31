CREATE TABLE @my_schema.@table_prefixtime_to_event (
    database_id varchar(100),
    target_cohort_definition_id bigint,
    outcome_cohort_definition_id bigint,
    outcome_type varchar(20),
    target_outcome_type varchar(20),
    time_to_event int,
    num_events int,
    time_scale varchar(20)
);

CREATE TABLE @my_schema.@table_prefixrechallenge_fail_case_series (
    --run_id,
    database_id varchar(100),
    dechallenge_stop_interval int,
    dechallenge_evaluation_window int,
    target_cohort_definition_id bigint,
    outcome_cohort_definition_id bigint,
    person_key int,
    subject_id bigint,
    dechallenge_exposure_number int,
    dechallenge_exposure_start_date_offset int,
    dechallenge_exposure_end_date_offset int,
    dechallenge_outcome_number int,
    dechallenge_outcome_start_date_offset int,
    rechallenge_exposure_number int,
    rechallenge_exposure_start_date_offset int,
    rechallenge_exposure_end_date_offset int,
    rechallenge_outcome_number int,
    rechallenge_outcome_start_date_offset int
);

CREATE TABLE @my_schema.@table_prefixdechallenge_rechallenge (
    database_id varchar(100),
    dechallenge_stop_interval int,
    dechallenge_evaluation_window int,
    target_cohort_definition_id bigint,
    outcome_cohort_definition_id bigint,
    num_exposure_eras int,
    num_persons_exposed int,
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
    pct_rechallenge_fail float
);


-- covariateSettings
CREATE TABLE @my_schema.@table_prefixsettings (
    run_id int,
    database_id varchar(100),
    covariate_setting_json varchar(MAX),
    risk_window_start int,
    start_anchor varchar(15),
    risk_window_end int,
    end_anchor varchar(15)
);

-- added this table
CREATE TABLE @my_schema.@table_prefixcohort_details (
    run_id int,
    database_id varchar(100),
    cohort_definition_id int,
    target_cohort_id int,
    outcome_cohort_id int,
    cohort_type varchar(10)
);

CREATE TABLE @my_schema.@table_prefixanalysis_ref (
    run_id int,
    database_id varchar(100),
    analysis_id int,
    analysis_name varchar(max),
    domain_id varchar(30),
    start_day int,
    end_day int,
    is_binary varchar(1),
    missing_means_zero varchar(1)
);

CREATE TABLE @my_schema.@table_prefixcovariate_ref (
    run_id int,
    database_id varchar(100),
    covariate_id int,
    covariate_name varchar(max),
    analysis_id int,
    concept_id int
);

CREATE TABLE @my_schema.@table_prefixcovariates (
    run_id int,
    database_id varchar(100),
    cohort_definition_id int,
    covariate_id int,
    sum_value int,
    average_value float
);

CREATE TABLE @my_schema.@table_prefixcovariates_continuous (
    run_id int,
    database_id varchar(100),
    cohort_definition_id int,
    covariate_id int,
    count_value int,
    min_value float,
    max_value float,
    average_value float,
    standard_deviation float,
    median_value float,
    p_10_value float,
    p_25_value float,
    p_75_value float,
    p_90_value float
);

CREATE TABLE @my_schema.@table_prefixcohort_counts(
    run_id int,
    database_id varchar(100),
    cohort_definition_id int,
    row_count int,
    person_count int
);
