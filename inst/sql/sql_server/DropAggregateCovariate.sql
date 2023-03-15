-- clean up by removing the temp tables

TRUNCATE TABLE #targets_agg_all;
DROP TABLE #targets_agg_all;

TRUNCATE TABLE #targets_agg;
DROP TABLE #targets_agg;

TRUNCATE TABLE #outcomes_agg;
DROP TABLE #outcomes_agg;

TRUNCATE TABLE #outcomes_agg_first;
DROP TABLE #outcomes_agg_first;

TRUNCATE TABLE #cohort_details;
DROP TABLE #cohort_details;

TRUNCATE TABLE #target_with_outcome;
DROP TABLE #target_with_outcome;

TRUNCATE TABLE #target_outcome_f;
DROP TABLE #target_outcome_f;

TRUNCATE TABLE #target_nooutcome;
DROP TABLE #target_nooutcome;

TRUNCATE TABLE #target_noout_f;
DROP TABLE #target_noout_f;

TRUNCATE TABLE #agg_cohorts;
DROP TABLE #agg_cohorts;










