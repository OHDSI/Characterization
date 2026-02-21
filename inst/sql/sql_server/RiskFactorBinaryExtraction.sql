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

SELECT *
FROM
(
SELECT
IFNULL(non_cases.characterization_case_id, cases.characterization_case_id) as characterization_case_id,
IFNULL(non_cases.covariate_id, cases.covariate_id) as covariate_id,
IFNULL(non_case_sum_value, 0) as non_case_sum_value,
IFNULL(case_sum_value, 0) as case_sum_value,
IFNULL(non_case_average_value, 0) as non_case_average_value,
IFNULL(case_average_value, 0) as case_average_value,
(IFNULL(case_average_value, 0.0) - IFNULL(non_case_average_value, 0.0))/
(SQRT(
  (
    (POWER((1.0 - IFNULL(case_average_value, 0.0)),2) * IFNULL(case_sum_value*1.0, 0.0)) +
    (POWER((0.0 - IFNULL(case_average_value, 0.0)),2) * (IFNULL(case_n*1.0, 0.0) - IFNULL(case_sum_value*1.0, 0.0)))
  )/IFNULL(case_n*1.0-1.0, 1.0)
+
  (
    (POWER((1.0 - IFNULL(non_case_average_value, 0.0)),2) * IFNULL(non_case_sum_value*1.0, 0.0)) +
    (POWER((0.0 - IFNULL(non_case_average_value, 0.0)),2) * (IFNULL(non_case_n*1.0, 0.0) - IFNULL(non_case_sum_value*1.0, 0)))
  )/IFNULL(non_case_n*1.0-1.0, 1.0)
)/2.0) as standardized_mean_difference


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

-- adding this to prevent zero division errors in sd
WHERE cases.case_n > 1 and non_cases.non_case_n > 1

) smd_table

WHERE  abs(smd_table.standardized_mean_difference) >= @smd_min
AND (IFNULL(non_case_sum_value, 0) + IFNULL(case_sum_value, 0) ) >= @min_count
;

