SELECT * FROM

(
SELECT
FLOOR(cohort_definition_id/10) as characterization_case_id,
covariate_id,

MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN count_value
  ELSE 0
END
) AS before_count_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN average_value
  ELSE 0
END
) AS before_average_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN min_value
  ELSE 0
END
) AS before_min_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN max_value
  ELSE 0
END
) AS before_max_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN median_value
  ELSE 0
END
) AS before_median_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN standard_deviation
  ELSE 0
END
) AS before_standard_deviation,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN p10_value
  ELSE 0
END
) AS before_p10_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 3 THEN p90_value
  ELSE 0
END
) AS before_p90_value,


MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN count_value
  ELSE 0
END
) AS during_count_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN average_value
  ELSE 0
END
) AS during_average_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN min_value
  ELSE 0
END
) AS during_min_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN max_value
  ELSE 0
END
) AS during_max_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN median_value
  ELSE 0
END
) AS during_median_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN standard_deviation
  ELSE 0
END
) AS during_standard_deviation,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN p10_value
  ELSE 0
END
) AS during_p10_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 4 THEN p90_value
  ELSE 0
END
) AS during_p90_value,


MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN count_value
  ELSE 0
END
) AS after_count_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN average_value
  ELSE 0
END
) AS after_average_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN min_value
  ELSE 0
END
) AS after_min_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN max_value
  ELSE 0
END
) AS after_max_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN median_value
  ELSE 0
END
) AS after_median_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN standard_deviation
  ELSE 0
END
) AS after_standard_deviation,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN p10_value
  ELSE 0
END
) AS after_p10_value,
MAX(
CASE
  WHEN (cohort_definition_id - FLOOR(cohort_definition_id/10)*10) = 5 THEN p90_value
  ELSE 0
END
) AS after_p90_value

FROM @characterization_fe_table
where cohort_definition_id in (@cohort_definition_ids)

GROUP BY
FLOOR(cohort_definition_id/10),
covariate_id

) main_table

WHERE (before_count_value + during_count_value + after_count_value) >= @min_count
;

