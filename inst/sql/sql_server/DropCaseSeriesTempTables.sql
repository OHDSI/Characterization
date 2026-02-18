-- clean up by removing the temp tables

TRUNCATE TABLE #fe_covariate_case;
DROP TABLE #fe_covariate_case;

TRUNCATE TABLE #fe_covariate_continuous_case;
DROP TABLE #fe_covariate_continuous_case;

TRUNCATE TABLE #fe_covariate_ref_case;
DROP TABLE #fe_covariate_ref_case;

TRUNCATE TABLE #fe_analysis_ref_case;
DROP TABLE #fe_analysis_ref_case;

TRUNCATE TABLE #fe_time_ref_case;
DROP TABLE #fe_time_ref_case;








