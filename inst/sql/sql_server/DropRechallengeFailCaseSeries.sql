-- clean up by removing the temp tables

TRUNCATE TABLE #target_cohort;
DROP TABLE #target_cohort;

TRUNCATE TABLE #outcome_cohort;
DROP TABLE #outcome_cohort;

TRUNCATE TABLE #fail_case_series;
DROP TABLE #fail_case_series;

