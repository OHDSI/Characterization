-- remove existing results
DELETE FROM @characterization_schema.@outcome_era_table
WHERE cohort_definition_id in (@outcome_ids)
AND outcome_washout = @outcome_washout
;

-- now determine the non-cases
  INSERT INTO @characterization_schema.@outcome_era_table(
    cohort_definition_id, outcome_washout, subject_id, cohort_start_date, cohort_end_date
  )

SELECT
  cohort_definition_id,
  @outcome_washout as outcome_washout,
  subject_id,
  MIN(cohort_start_date) AS cohort_start_date,
  MAX(cohort_end_date) AS cohort_end_date

FROM (

  SELECT
    cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date,

    SUM(
     CASE WHEN previous_cohort_end_date >= cohort_start_date THEN 0
     ELSE 1
     END)
    OVER (
      PARTITION BY cohort_definition_id, subject_id
      ORDER BY cohort_start_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS era_count

  FROM (
    SELECT
      cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date,
      MAX(DATEADD(day, @outcome_washout, cohort_end_date)) OVER (
        PARTITION BY cohort_definition_id, subject_id
        ORDER BY cohort_start_date ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ) AS previous_cohort_end_date
    FROM @cohort_schema.@cohort_table

    WHERE cohort_definition_id IN (@outcome_ids)
  ) prior_eras

) temp

GROUP BY
cohort_definition_id,
subject_id,
era_count;
