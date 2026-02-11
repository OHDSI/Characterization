SELECT
FLOOR(cohort_definition_id/10) as characterization_case_id,
covariate_id,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN sum_value
  ELSE 0
END
) AS before_sum_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN average_value
  ELSE 0
END
) AS before_average_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN sum_value
  ELSE 0
END
) AS during_sum_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN average_value
  ELSE 0
END
) AS during_average_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN sum_value
  ELSE 0
END
) AS after_sum_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN average_value
  ELSE 0
END
) AS after_average_value

FROM @characterization_fe_table
where (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) in (3,4,5)

GROUP BY
FLOOR(cohort_definition_id/10),
covariate_id;

