WITH

cohort_of_int AS (

SELECT
characterization_case_id,

{@efficient_mode}?{
characterization_target_id as non_case_id,
}:{
characterization_case_id*10+2 as non_case_id,
}

characterization_case_id*10+1 as case_id
FROM @characterization_schema.@case_settings_table
WHERE characterization_case_id in (@characterization_case_ids)
),

char_counts AS (
SELECT cohort_definition_id, count(*) as n
FROM @characterization_schema.@characterization_table
WHERE cohort_definition_id in (
SELECT non_case_id FROM cohort_of_int
UNION
SELECT case_id FROM cohort_of_int
)
GROUP BY cohort_definition_id
)

SELECT *,
CASE WHEN st_dev = 0 THEN mean_diff ELSE mean_diff/st_dev END as standardized_mean_difference

FROM
(
SELECT
ISNULL(non_cases.characterization_case_id, cases.characterization_case_id) as characterization_case_id,
ISNULL(non_cases.covariate_id, cases.covariate_id) as covariate_id,
ISNULL(non_case_sum_value, 0) as non_case_sum_value,
ISNULL(case_sum_value, 0) as case_sum_value,
ISNULL(non_case_average_value, 0) as non_case_average_value,
ISNULL(case_average_value, 0) as case_average_value,
(ISNULL(case_average_value, 0.0) - ISNULL(non_case_average_value, 0.0))*1.0 as mean_diff,
SQRT(
(
  (
    (POWER((1.0 - ISNULL(case_average_value, 0.0)),2) * ISNULL(case_sum_value*1.0, 0.0)) +
    (POWER((0.0 - ISNULL(case_average_value, 0.0)),2) * (ISNULL(case_n*1.0, 0.0) - ISNULL(case_sum_value*1.0, 0.0)))
  )/CASE WHEN ISNULL(case_n*1.0-1.0, 1.0) = 0 THEN 1.0 ELSE ISNULL(case_n*1.0-1.0, 1.0) END

+
  (
    (POWER((1.0 - ISNULL(non_case_average_value, 0.0)),2) * ISNULL(non_case_sum_value*1.0, 0.0)) +
    (POWER((0.0 - ISNULL(non_case_average_value, 0.0)),2) * (ISNULL(non_case_n*1.0, 0.0) - ISNULL(non_case_sum_value*1.0, 0)))
  )/CASE WHEN ISNULL(non_case_n*1.0-1.0, 1.0) = 0 THEN 1.0 ELSE ISNULL(non_case_n*1.0-1.0, 1.0) END

  )/2.0
  ) as st_dev


FROM

(SELECT
counts.n as non_case_n,
coi.characterization_case_id,
covariate_id,
sum_value as non_case_sum_value,
average_value as non_case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
INNER JOIN cohort_of_int coi
ON coi.non_case_id = counts.cohort_definition_id
) non_cases

FULL JOIN

(SELECT
counts.n as case_n,
coi.characterization_case_id,
covariate_id,
sum_value as case_sum_value,
average_value as case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
INNER JOIN cohort_of_int coi
ON coi.case_id = counts.cohort_definition_id

) cases

ON non_cases.characterization_case_id = cases.characterization_case_id
AND non_cases.covariate_id = cases.covariate_id

) smd_table

WHERE  abs(CASE WHEN st_dev = 0 THEN mean_diff/0.0000001 ELSE mean_diff/st_dev END) >= @smd_min
AND (ISNULL(non_case_sum_value, 0) + ISNULL(case_sum_value, 0) ) >= @min_count
;

