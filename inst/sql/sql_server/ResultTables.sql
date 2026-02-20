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

CREATE TABLE @my_schema.@table_prefixanalysis_ref (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    analysis_id int NOT NULL,
    analysis_name varchar(max) NOT NULL,
    domain_id varchar(30),
    start_day int,
    end_day int,
    is_binary varchar(1),
    missing_means_zero varchar(1),
    PRIMARY KEY (database_id, setting_id, analysis_id)
);

CREATE TABLE @my_schema.@table_prefixcovariate_ref (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    covariate_id bigint NOT NULL,
    covariate_name varchar(max) NOT NULL,
    analysis_id int NOT NULL,
    concept_id bigint,
    value_as_concept_id int,
    collisions int,
    PRIMARY KEY (database_id, setting_id, covariate_id)
);

-- TARGETS
CREATE TABLE @my_schema.@table_prefixtarget_covariates (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    characterization_target_id bigint,
    covariate_id bigint NOT NULL,
    sum_value bigint NOT NULL,
    average_value float,
    PRIMARY KEY (database_id, setting_id, characterization_target_id, covariate_id)
);

CREATE TABLE @my_schema.@table_prefixtarget_covariates_continuous (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    characterization_target_id bigint,
    covariate_id bigint NOT NULL,
    count_value bigint NOT NULL,
    min_value float,
    max_value float,
    average_value float,
    standard_deviation float,
    median_value float,
    p_10_value float,
    p_25_value float,
    p_75_value float,
    p_90_value float,
    PRIMARY KEY (database_id, setting_id, characterization_target_id, covariate_id)
);

-- RISK FACTOR
CREATE TABLE @my_schema.@table_prefixrisk_factor_covariates (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    characterization_case_id bigint NOT NULL,
    covariate_id bigint NOT NULL,
    non_case_sum_value bigint NOT NULL,
    non_case_average_value float NOT NULL,
    case_sum_value bigint NOT NULL,
    case_average_value float NOT NULL,
    standardized_mean_difference float,
    PRIMARY KEY (database_id, setting_id, characterization_case_id, covariate_id)
);

CREATE TABLE @my_schema.@table_prefixrisk_factor_covariates_continuous (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    characterization_case_id BIGINT NOT NULL,
    covariate_id bigint NOT NULL,
    case_count_value bigint NOT NULL,
    case_min_value float NOT NULL,
    case_max_value float NOT NULL,
    case_average_value float NOT NULL,
    case_standard_deviation float NOT NULL,
    case_median_value float NOT NULL,
    case_p_10_value float NOT NULL,
    case_p_25_value float NOT NULL,
    case_p_75_value float NOT NULL,
    case_p_90_value float NOT NULL,
    non_case_count_value bigint NOT NULL,
    non_case_min_value float NOT NULL,
    non_case_max_value float NOT NULL,
    non_case_average_value bigint NOT NULL,
    non_case_standard_deviation float NOT NULL,
    non_case_median_value float NOT NULL,
    non_case_p_10_value float NOT NULL,
    non_case_p_25_value float NOT NULL,
    non_case_p_75_value float NOT NULL,
    non_case_p_90_value float NOT NULL,
    standardized_mean_difference float,
    PRIMARY KEY (database_id, setting_id, characterization_case_id, covariate_id)
);


-- CASE SERIES
CREATE TABLE @my_schema.@table_prefixcase_series_covariates (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    characterization_case_id bigint NOT NULL,
    covariate_id bigint NOT NULL,
    before_sum_value bigint NOT NULL,
    before_average_value float NOT NULL,
    during_sum_value bigint NOT NULL,
    during_average_value float NOT NULL,
    after_sum_value bigint NOT NULL,
    after_average_value float NOT NULL,
    PRIMARY KEY (database_id, setting_id, characterization_case_id, covariate_id)
);

CREATE TABLE @my_schema.@table_prefixcase_series_covariates_continuous (
    database_id varchar(100) NOT NULL,
    setting_id varchar(50) NOT NULL,
    characterization_case_id BIGINT NOT NULL,
    covariate_id bigint NOT NULL,
    before_count_value bigint NOT NULL,
    before_min_value float NOT NULL,
    before_max_value float NOT NULL,
    before_average_value float NOT NULL,
    before_standard_deviation float NOT NULL,
    before_median_value float NOT NULL,
    before_p_10_value float NOT NULL,
    before_p_90_value float NOT NULL,
    during_count_value bigint NOT NULL,
    during_min_value float NOT NULL,
    during_max_value float NOT NULL,
    during_average_value bigint NOT NULL,
    during_standard_deviation float NOT NULL,
    during_median_value float NOT NULL,
    during_p_10_value float NOT NULL,
    during_p_90_value float NOT NULL,
    after_count_value bigint NOT NULL,
    after_min_value float NOT NULL,
    after_max_value float NOT NULL,
    after_average_value bigint NOT NULL,
    after_standard_deviation float NOT NULL,
    after_median_value float NOT NULL,
    after_p_10_value float NOT NULL,
    after_p_90_value float NOT NULL,
    PRIMARY KEY (database_id, setting_id, characterization_case_id, covariate_id)
);

-- SETTINGS
CREATE TABLE @my_schema.@table_prefixexecution_settings (
    setting_id varchar(50) NOT NULL,
    database_id varchar(100) NOT NULL,
    database_hash varchar(50) NOT NULL,
    mode varchar(15),
    min_characterization_mean FLOAT,
    min_covariate_count INT,
    min_smd FLOAT,
    PRIMARY KEY (setting_id, database_id)
);

CREATE TABLE @my_schema.@table_prefixtarget_settings (
    setting_id varchar(50) NOT NULL,
    database_id varchar(100) NOT NULL,
    characterization_target_id BIGINT NOT NULL,
    target_id BIGINT,
    limit_to_first_in_n_days INT,
    min_prior_observation INT,
    PRIMARY KEY (setting_id, database_id,characterization_target_id)
);

CREATE TABLE @my_schema.@table_prefixcase_settings (
    setting_id varchar(50) NOT NULL,
    database_id varchar(100) NOT NULL,
    characterization_case_id BIGINT NOT NULL,
    characterization_target_id BIGINT,
    outcome_id BIGINT,
    outcome_washout_days INT,
    start_anchor VARCHAR(15),
    end_anchor VARCHAR(15),
    risk_window_start INT,
    risk_window_end INT,
    runtype VARCHAR(50),
    PRIMARY KEY (setting_id, database_id,characterization_case_id)
);

CREATE TABLE @my_schema.@table_prefixcase_series_settings (
    setting_id varchar(50) NOT NULL,
    case_pre_target_duration int,
    case_post_outcome_duration int,
    PRIMARY KEY (setting_id)
);

-- added this table
CREATE TABLE @my_schema.@table_prefixattrition (
    database_id varchar(100) NOT NULL,
    setting_id varchar(30) NOT NULL,
    cohort_definition_id BIGINT,
    attr_reason VARCHAR(200),
    n BIGINT,
    PRIMARY KEY (setting_id, database_id, cohort_definition_id, attr_reason)
);
