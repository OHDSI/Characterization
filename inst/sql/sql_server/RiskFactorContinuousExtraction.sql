
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

SELECT
characterization_case_id,
covariate_id,
non_case_count_value,
case_count_value,
non_case_min_value,
case_min_value,
non_case_max_value,
case_max_value,
non_case_average_value,
case_average_value,
non_case_median_value,
case_median_value,
non_case_p10_value,
case_p10_value,
non_case_p25_value,
case_p25_value,
non_case_p75_value,
case_p75_value,
non_case_p90_value,
case_p90_value,
non_case_standard_deviation,
case_standard_deviation,
CASE WHEN st_dev = 0 THEN mean_diff ELSE mean_diff/st_dev END as standardized_mean_difference


FROM
(
SELECT
ISNULL(non_cases.characterization_case_id, cases.characterization_case_id) as characterization_case_id,
ISNULL(non_cases.covariate_id, cases.covariate_id) as covariate_id,
ISNULL(non_case_count_value, 0) as non_case_count_value,
ISNULL(case_count_value, 0) as case_count_value,
ISNULL(non_case_min_value, 0) as non_case_min_value,
ISNULL(case_min_value, 0) as case_min_value,
ISNULL(non_case_max_value, 0) as non_case_max_value,
ISNULL(case_max_value, 0) as case_max_value,
ISNULL(non_case_average_value, 0) as non_case_average_value,
ISNULL(case_average_value, 0) as case_average_value,
ISNULL(non_case_median_value, 0) as non_case_median_value,
ISNULL(case_median_value, 0) as case_median_value,
ISNULL(non_case_p10_value, 0) as non_case_p10_value,
ISNULL(case_p10_value, 0) as case_p10_value,
ISNULL(non_case_p25_value, 0) as non_case_p25_value,
ISNULL(case_p25_value, 0) as case_p25_value,
ISNULL(non_case_p75_value, 0) as non_case_p75_value,
ISNULL(case_p75_value, 0) as case_p75_value,
ISNULL(non_case_p90_value, 0) as non_case_p90_value,
ISNULL(case_p90_value, 0) as case_p90_value,
ISNULL(non_case_standard_deviation, 0) as non_case_standard_deviation,
ISNULL(case_standard_deviation, 0) as case_standard_deviation,
(ISNULL(case_average_value, 0.0) - ISNULL(non_case_average_value, 0.0))*1.0 as mean_diff,
SQRT(
(POWER(ISNULL(case_standard_deviation, 0.0),2) + POWER(ISNULL(non_case_standard_deviation, 0.0),2))
/2.0) as st_dev


FROM

(SELECT
counts.n as non_case_n,
coi.characterization_case_id,
covariate_id,
count_value as non_case_count_value,
min_value as non_case_min_value,
max_value as non_case_max_value,
standard_deviation as non_case_standard_deviation,
median_value as non_case_median_value,
p10_value as non_case_p10_value,
p25_value as non_case_p25_value,
p75_value as non_case_p75_value,
p90_value as non_case_p90_value,
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
count_value as case_count_value,
min_value as case_min_value,
max_value as case_max_value,
standard_deviation as case_standard_deviation,
median_value as case_median_value,
p10_value as case_p10_value,
p25_value as case_p25_value,
p75_value as case_p75_value,
p90_value as case_p90_value,
average_value as case_average_value
FROM @characterization_fe_table INNER JOIN char_counts counts
ON @characterization_fe_table.cohort_definition_id = counts.cohort_definition_id
INNER JOIN cohort_of_int coi
ON coi.case_id = counts.cohort_definition_id
) cases

ON non_cases.characterization_case_id = cases.characterization_case_id
AND non_cases.covariate_id = cases.covariate_id

) temp

WHERE  abs(CASE WHEN st_dev = 0 THEN mean_diff ELSE mean_diff/st_dev END) >= @smd_min
AND (ISNULL(non_case_count_value, 0) + ISNULL(case_count_value, 0) ) >= @min_count
;
