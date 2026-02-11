-- drop temp table at end
--IF OBJECT_ID('tempdb..#char_counts', 'U') IS NOT NULL DROP TABLE #char_counts;

WITH char_counts AS (
SELECT cohort_definition_id, count(*) as n
FROM @characterization_schema.@characterization_table
WHERE cohort_definition_id in (@cohort_definition_ids)
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
SQRT(
  (
    (POWER((1.0 - IFNULL(case_average_value, 0.0)),2) * IFNULL(case_sum_value*1.0, 0.0)) +
    (POWER((0.0 - IFNULL(case_average_value, 0.0)),2) * (IFNULL(case_n*1.0, 0.0) - IFNULL(case_sum_value*1.0, 0.0)))
  )/IFNULL(case_n*1.0, 1.0)
)
+
SQRT(
  (
    (POWER((1.0 - IFNULL(non_case_average_value, 0.0)),2) * IFNULL(non_case_sum_value*1.0, 0.0)) +
    (POWER((0.0 - IFNULL(non_case_average_value, 0.0)),2) * (IFNULL(non_case_n*1.0, 0.0) - IFNULL(non_case_sum_value*1.0, 0)))
  )/IFNULL(non_case_n*1.0, 1.0)
)
)/2.0) as standardized_mean_difference


FROM

(SELECT
counts.n as non_case_n,
cs.characterization_case_id,
covariate_id,
sum_value as non_case_sum_value,
average_value as non_case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
INNER JOIN @case_settings_table cs
{@efficient_mode}?{
ON cs.characterization_target_id = counts.cohort_definition_id
}:{
ON cs.characterization_case_id = FLOOR(counts.cohort_definition_id/10)
}
WHERE (counts.cohort_definition_id - FLOOR(counts.cohort_definition_id/10)*10) = {@efficient_mode}?{0}:{2}

) non_cases

FULL JOIN

(SELECT
counts.n as case_n,
FLOOR(counts.cohort_definition_id/10) as characterization_case_id,
covariate_id,
sum_value as case_sum_value,
average_value as case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
where (counts.cohort_definition_id - FLOOR(counts.cohort_definition_id/10)*10) = 1) cases

ON non_cases.characterization_case_id = cases.characterization_case_id
AND non_cases.covariate_id = cases.covariate_id

) temp

WHERE  abs(temp.standardized_mean_difference) >= @smd_min;

--IF OBJECT_ID('tempdb..#char_counts', 'U') IS NOT NULL DROP TABLE #char_counts;


