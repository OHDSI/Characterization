-- clean up by removing the temp tables

TRUNCATE TABLE #targets_all;
DROP TABLE #targets_all;

TRUNCATE TABLE #targets_inclusions;
DROP TABLE #targets_inclusions;

TRUNCATE TABLE #cohort_details;
DROP TABLE #cohort_details;

TRUNCATE TABLE #agg_cohorts_before;
DROP TABLE #agg_cohorts_before;

TRUNCATE TABLE #agg_cohorts_extras;
DROP TABLE #agg_cohorts_extras;








