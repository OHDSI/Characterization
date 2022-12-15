-- clean up by removing the temp tables

TRUNCATE TABLE #cohort_settings;
DROP TABLE #cohort_settings;

TRUNCATE TABLE #targets;
DROP TABLE #targets;

TRUNCATE TABLE #outcomes;
DROP TABLE #outcomes;

TRUNCATE TABLE #target_w_outcome;
DROP TABLE #target_w_outcome;

TRUNCATE TABLE #two_fu_bounds;
DROP TABLE #two_fu_bounds;

TRUNCATE TABLE #t_prior_obs;
DROP TABLE #t_prior_obs;

TRUNCATE TABLE #t_post_obs;
DROP TABLE #t_post_obs;

TRUNCATE TABLE #two_tte;
DROP TABLE #two_tte;

TRUNCATE TABLE #two_tte_summary;
DROP TABLE #two_tte_summary;










