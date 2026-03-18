-- clean up by removing the temp tables

TRUNCATE TABLE #fe_covariate_rf;
DROP TABLE #fe_covariate_rf;

TRUNCATE TABLE #fe_covariate_continuous_rf;
DROP TABLE #fe_covariate_continuous_rf;

TRUNCATE TABLE #fe_covariate_ref_rf;
DROP TABLE #fe_covariate_ref_rf;

TRUNCATE TABLE #fe_analysis_ref_rf;
DROP TABLE #fe_analysis_ref_rf;

DROP TABLE IF EXISTS #fe_time_ref_rf;








