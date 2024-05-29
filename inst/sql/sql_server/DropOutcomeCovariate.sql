-- clean up by removing the temp tables

TRUNCATE TABLE #outcomes_all;
DROP TABLE #outcomes_all;

TRUNCATE TABLE #outcomes_washout;
DROP TABLE #outcomes_washout;

TRUNCATE TABLE #cohort_details;
DROP TABLE #cohort_details;

TRUNCATE TABLE #agg_cohorts_before;
DROP TABLE #agg_cohorts_before;

TRUNCATE TABLE #agg_cohorts_extras;
DROP TABLE #agg_cohorts_extras;








