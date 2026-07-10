-- clean this table at the end
IF OBJECT_ID('tempdb..#temp_non_cases', 'U') IS NOT NULL DROP TABLE #temp_non_cases;

SELECT
    case_settings.characterization_case_id*10+2 as cohort_definition_id,
    t.row_number,
    t.subject_id,
    t.cohort_start_date,
    t.cohort_end_date,
    -- has no tar ignoring outcome washout (CI excluded)
    MAX(CASE WHEN dateadd(day, @risk_window_start, t.@start_anchor_date) > dateadd(day, @risk_window_end, t.@end_anchor_date) THEN 1 else 0 END) AS no_tar_ignoring_outcome_washout,
    MAX(CASE WHEN dateadd(day, @risk_window_start, t.@start_anchor_date) > t.observation_period_end_date THEN 1 else 0 END) AS no_tar_obs,
    -- has no tar due to outcome washout (CI excluded but in pe)
    MAX(CASE WHEN DATEDIFF(day,dateadd(day,@outcome_washout, o.cohort_end_date) , dateadd(day, @risk_window_end, t.@end_anchor_date)) <= 0 THEN 1 ELSE 0 END) AS no_tar_because_outcome_washout,
    -- leaves database leading to no TAR
    MAX(CASE WHEN DATEDIFF(day,dateadd(day,@outcome_washout, o.cohort_end_date) , t.observation_period_end_date) <= 0 THEN 1 ELSE 0 END) AS no_tar_washout_and_obs,
    -- outcome overlaps the washout before tar (used by plp to exclude)
    MAX(CASE
    WHEN o.cohort_start_date IS NOT NULL
    AND o.cohort_start_date < dateadd(day, @risk_window_start, t.@start_anchor_date)
    -- Note: should it be > or >= ? this will remove overlapping even when washout is 0
    AND o.cohort_end_date >= dateadd(day, -@outcome_washout, dateadd(day, @risk_window_start, t.@start_anchor_date))
    THEN 1 else 0 END) AS outcome_in_washout_before_tar,

    -- ADD has outcome in TAR (left join CASES on characterization_target_id, row_id and )
    MAX(CASE WHEN cases.row_number IS NOT NULL THEN 1 else 0 END) AS outcome_during_tar

    INTO #temp_non_cases
    FROM @characterization_schema.@characterization_table t

        INNER JOIN
    (SELECT * FROM @case_settings_schema.@case_settings_table
      WHERE outcome_washout_days = @outcome_washout
      AND risk_window_start = @risk_window_start
      AND risk_window_end = @risk_window_end
      AND start_anchor = '@start_anchor'
      AND end_anchor = '@end_anchor'
    ) case_settings
    ON t.cohort_definition_id = case_settings.characterization_target_id

    LEFT JOIN @cohort_schema.@cohort_table o
    ON t.subject_id = o.subject_id
    AND case_settings.outcome_id = o.cohort_definition_id

    {@restrict_washout_to_obs}?{
      AND o.cohort_start_date >= t.observation_period_start_date
      AND o.cohort_start_date <= t.observation_period_end_date
    }
    -- outcome starts before TAR start
    AND o.cohort_start_date < dateadd(day, @risk_window_start, t.@start_anchor_date)
    -- outcome end after washout prior before TAR start
    AND o.cohort_end_date >= dateadd(day, -@outcome_washout, dateadd(day, @risk_window_start, t.@start_anchor_date))

    -- use real table not temp case table?
    LEFT JOIN (SELECT * from @characterization_schema.@characterization_table
    WHERE char_type = 'cases' ) cases
    ON cases.cohort_definition_id = case_settings.characterization_case_id*10+1
    AND cases.row_number = t.row_number

    WHERE case_settings.outcome_id IN (@outcome_cohort_ids)
    AND case_settings.characterization_target_id IN (@characterization_target_ids)


    GROUP BY
    case_settings.characterization_case_id,
    t.row_number,
    t.subject_id,
    t.cohort_start_date,
    t.cohort_end_date;

-- remove existing results
DELETE FROM @characterization_schema.@characterization_table
WHERE char_type = 'non-cases'
AND cohort_definition_id in (SELECT DISTINCT cohort_definition_id FROM #temp_non_cases)
;

-- now determine the non-cases
  INSERT INTO @characterization_schema.@characterization_table(
    cohort_definition_id, row_number, subject_id, cohort_start_date, cohort_end_date, char_type
  )

  SELECT
  temp.cohort_definition_id,
  temp.row_number,
  temp.subject_id,
  temp.cohort_start_date,
  temp.cohort_end_date,
  'non-cases' as char_type

  FROM #temp_non_cases temp

  -- not a case
  WHERE temp.outcome_during_tar = 0

  {@use_plp}?{ -- exclude anyone with with outcome during washout before TAR or no TAR
    AND temp.outcome_in_washout_before_tar = 0
    AND temp.no_tar_ignoring_outcome_washout = 0
    AND temp.no_tar_obs = 0
  }
  {@use_ci}?{ -- exclude anyone without 1+ days of TAR
    AND temp.no_tar_ignoring_outcome_washout = 0
    AND temp.no_tar_because_outcome_washout = 0
    AND temp.no_tar_washout_and_obs = 0
    AND temp.no_tar_obs = 0
  }

  ;



INSERT INTO @characterization_schema.@attrition_table

SELECT
cohort_definition_id,
attr_reason,
count(*) as n

FROM

(SELECT
  temp.cohort_definition_id,
  temp.row_number,

{@use_plp}?{
CASE
WHEN temp.no_tar_ignoring_outcome_washout = 1 OR temp.no_tar_obs = 1 THEN '1. No TAR due to TAR start > TAR end or observation end'
WHEN temp.outcome_in_washout_before_tar = 1 THEN '2. Outcome occurs during washout'
WHEN temp.outcome_during_tar = 1 THEN '3. Has outcome during TAR'
END attr_reason
}
{@use_ci}?{
CASE
WHEN temp.no_tar_ignoring_outcome_washout = 1 OR temp.no_tar_obs = 1 THEN '1. No TAR due to TAR start > TAR end or observation end'
WHEN temp.no_tar_because_outcome_washout = 1 OR temp.no_tar_washout_and_obs = 1 THEN '2. No TAR due to outcome washout'
WHEN temp.outcome_during_tar = 1 THEN '3. Has outcome during TAR'
END attr_reason
}

FROM #temp_non_cases temp
) attrition

WHERE attr_reason IS NOT NULL

GROUP BY
cohort_definition_id,
attr_reason
;

-- cleaning table
IF OBJECT_ID('tempdb..#temp_non_cases', 'U') IS NOT NULL DROP TABLE #temp_non_cases;

