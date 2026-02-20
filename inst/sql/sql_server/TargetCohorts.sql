-- first entry in washout days and min prior obs

IF OBJECT_ID('tempdb..#temp_target', 'U') IS NOT NULL DROP TABLE #temp_target;

SELECT
target_settings.characterization_target_id AS cohort_definition_id,
row_number() over(PARTITION BY CAST(target_settings.characterization_target_id AS BIGINT) ORDER BY temp_cohort.subject_id, temp_cohort.cohort_start_date ASC) AS row_number,
temp_cohort.subject_id,
temp_cohort.cohort_start_date,
temp_cohort.cohort_end_date,
op.observation_period_start_date,
op.observation_period_end_date,
'target' as char_type

INTO #temp_target

FROM (SELECT
      cohort_definition_id,
      @limit_to_first_in_n_days AS limit_to_first_in_n_days,
      @min_prior_observation AS min_prior_observation,
      subject_id,
      cohort_start_date,
      cohort_end_date,
      ISNULL(datediff(day, LAG(cohort_start_date) OVER(partition BY subject_id, cohort_definition_id ORDER BY cohort_start_date ASC), cohort_start_date ), -1) AS time_between
      FROM @cohort_schema.@cohort_table WHERE cohort_definition_id IN (@cohort_ids)
) temp_cohort

INNER JOIN @cdm_database_schema.observation_period op
ON op.person_id = temp_cohort.subject_id
AND temp_cohort.cohort_start_date >= op.observation_period_start_date
AND temp_cohort.cohort_start_date <= op.observation_period_end_date

INNER JOIN
(SELECT * FROM @target_settings_schema.@target_settings_table
 WHERE limit_to_first_in_n_days = @limit_to_first_in_n_days
 AND min_prior_observation = @min_prior_observation
) target_settings
ON temp_cohort.cohort_definition_id = target_settings.target_id

WHERE (temp_cohort.time_between >= @limit_to_first_in_n_days OR temp_cohort.time_between = -1)
AND datediff(day, op.observation_period_start_date, temp_cohort.cohort_start_date) >= @min_prior_observation;

-- remove existing rows with cohort ids
DELETE FROM @characterization_schema.@characterization_table
WHERE char_type = 'target'
AND cohort_definition_id in (SELECT DISTINCT cohort_definition_id FROM #temp_target)
;

-- insert the new rows
-- now determine the non-cases
  INSERT INTO @characterization_schema.@characterization_table(
    cohort_definition_id, row_number, subject_id, cohort_start_date, cohort_end_date,
    observation_period_start_date, observation_period_end_date, char_type
  )

  SELECT
  temp.cohort_definition_id,
  temp.row_number,
  temp.subject_id,
  temp.cohort_start_date,
  temp.cohort_end_date,
  temp.observation_period_start_date,
  temp.observation_period_end_date,
  'target' as char_type

  FROM #temp_target temp;


INSERT INTO @characterization_schema.@attrition_table

SELECT
cohort_definition_id,
'Target first in @limit_to_first_in_n_days - @min_prior_observation prior obs' as attr_reason,
count(*) as N

FROM #temp_target

GROUP BY
cohort_definition_id
;

-- clean up
IF OBJECT_ID('tempdb..#temp_target', 'U') IS NOT NULL DROP TABLE #temp_target;
