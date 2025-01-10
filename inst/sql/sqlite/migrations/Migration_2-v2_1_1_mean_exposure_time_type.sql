-- Database migrations for verion 2.1.1

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}

-- Change the column type of mean_exposure_time
ALTER TABLE @database_schema.@table_prefixcohort_counts RENAME TO _cohort_counts_old;

CREATE TABLE @database_schema.@table_prefixcohort_counts (
    database_id varchar(100) NOT NULL,
    cohort_type varchar(10),
    target_cohort_id int,
    outcome_cohort_id int,
    risk_window_start int,
    risk_window_end int,
    start_anchor varchar(15),
    end_anchor varchar(15),
    min_prior_observation int,
    outcome_washout_days int,
    row_count int NOT NULL,
    person_count int NOT NULL,
    min_exposure_time int,
    mean_exposure_time float,
    max_exposure_time int
);

INSERT INTO @database_schema.@table_prefixcohort_counts
            (database_id, cohort_type, target_cohort_id, outcome_cohort_id,
             risk_window_start, risk_window_end, start_anchor, end_anchor,
             min_prior_observation, outcome_washout_days,row_count, person_count,
             min_exposure_time, mean_exposure_time, max_exposure_time)
SELECT database_id, cohort_type, target_cohort_id, outcome_cohort_id,
       risk_window_start, risk_window_end, start_anchor, end_anchor,
       min_prior_observation, outcome_washout_days,row_count, person_count,
       min_exposure_time, mean_exposure_time, max_exposure_time
FROM _cohort_counts_old;

DROP TABLE _cohort_counts_old;
