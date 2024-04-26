-- clean up by removing the temp tables

TRUNCATE TABLE #targets_all;
DROP TABLE #targets_all;

TRUNCATE TABLE #targets_inclusions;
DROP TABLE #targets_inclusions;

TRUNCATE TABLE #outcomes_all;
DROP TABLE #outcomes_all;

TRUNCATE TABLE #outcomes_washout;
DROP TABLE #outcomes_washout;

TRUNCATE TABLE #cohort_details;
DROP TABLE #cohort_details;

TRUNCATE TABLE #target_outcome_tar;
DROP TABLE #target_outcome_tar;

TRUNCATE TABLE #target_outcome_prior;
DROP TABLE #target_outcome_prior;

TRUNCATE TABLE #agg_cohorts_before;
DROP TABLE #agg_cohorts_before;

TRUNCATE TABLE #agg_cohorts_between;
DROP TABLE #agg_cohorts_between;

TRUNCATE TABLE #agg_cohorts_after;
DROP TABLE #agg_cohorts_after;

TRUNCATE TABLE #agg_cohorts_extras;
DROP TABLE #agg_cohorts_extras;








