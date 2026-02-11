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
IFNULL(non_case_count_value, 0) as non_case_count_value,
IFNULL(case_count_value, 0) as case_count_value,
IFNULL(non_case_min_value, 0) as non_case_min_value,
IFNULL(case_min_value, 0) as case_min_value,
IFNULL(non_case_max_value, 0) as non_case_max_value,
IFNULL(case_max_value, 0) as case_max_value,
IFNULL(non_case_average_value, 0) as non_case_average_value,
IFNULL(case_average_value, 0) as case_average_value,
IFNULL(non_case_median_value, 0) as non_case_median_value,
IFNULL(case_median_value, 0) as case_median_value,
IFNULL(non_case_p_10_value, 0) as non_case_p_10_value,
IFNULL(case_p_10_value, 0) as case_p_10_value,
IFNULL(non_case_p_25_value, 0) as non_case_p_25_value,
IFNULL(case_p_25_value, 0) as case_p_25_value,
IFNULL(non_case_p_75_value, 0) as non_case_p_75_value,
IFNULL(case_p_75_value, 0) as case_p_75_value,
IFNULL(non_case_p_90_value, 0) as non_case_p_90_value,
IFNULL(case_p_90_value, 0) as case_p_90_value,
IFNULL(non_case_standard_deviation, 0) as non_case_standard_deviation,
IFNULL(case_standard_deviation, 0) as case_standard_deviation,
(IFNULL(case_average_value, 0.0) - IFNULL(non_case_average_value, 0.0))/
SQRT(
(POWER(IFNULL(case_standard_deviation, 0.0),2) + POWER(IFNULL(non_case_standard_deviation, 0.0),2))
/2.0) as standardized_mean_difference


FROM

(SELECT
counts.n as non_case_n,
FLOOR(counts.cohort_definition_id/10) as characterization_case_id,
covariate_id,
count_value as non_case_count_value,
min_value as non_case_min_value,
max_value as non_case_max_value,
standard_deviation as non_case_standard_deviation,
median_value as non_case_median_value,
p_10_value as non_case_p_10_value,
p_25_value as non_case_p_25_value,
p_75_value as non_case_p_75_value,
p_90_value as non_case_p_90_value,
average_value as non_case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
where (counts.cohort_definition_id - FLOOR(counts.cohort_definition_id/10)*10) = {@efficient_mode}?{0}:{2}) non_cases

FULL JOIN

(SELECT
counts.n as case_n,
FLOOR(counts.cohort_definition_id/10) as characterization_case_id,
covariate_id,
count_value as case_count_value,
min_value as case_min_value,
max_value as case_max_value,
standard_deviation as case_standard_deviation,
median_value as case_median_value,
p_10_value as case_p_10_value,
p_25_value as case_p_25_value,
p_75_value as case_p_75_value,
p_90_value as case_p_90_value,
average_value as case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
where (counts.cohort_definition_id - FLOOR(counts.cohort_definition_id/10)*10) = 1) cases

ON non_cases.characterization_case_id = cases.characterization_case_id
AND non_cases.covariate_id = cases.covariate_id

) temp

WHERE  abs(temp.standardized_mean_difference) >= @smd_min;

--IF OBJECT_ID('tempdb..#char_counts', 'U') IS NOT NULL DROP TABLE #char_counts;


