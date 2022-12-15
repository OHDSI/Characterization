-- clean up by removing the temp tables

TRUNCATE TABLE #target_cohort;
DROP TABLE #target_cohort;

TRUNCATE TABLE #outcome_cohort;
DROP TABLE #outcome_cohort;

TRUNCATE TABLE #challenge;
DROP TABLE #challenge;
