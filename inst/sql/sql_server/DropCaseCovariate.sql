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

TRUNCATE TABLE #cases_tar;
DROP TABLE #cases_tar;

TRUNCATE TABLE #case_exclude;
DROP TABLE #case_exclude;

TRUNCATE TABLE #cases;
DROP TABLE #cases;

TRUNCATE TABLE #case_series;
DROP TABLE #case_series;








