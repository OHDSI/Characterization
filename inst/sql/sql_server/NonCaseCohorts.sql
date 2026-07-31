-- clean this table at the end
IF OBJECT_ID('tempdb..#temp_non_cases', 'U') IS NOT NULL DROP TABLE #temp_non_cases;
IF OBJECT_ID('tempdb..#temp_non_cases_with_tar', 'U') IS NOT NULL DROP TABLE #temp_non_cases_with_tar;
IF OBJECT_ID('tempdb..#temp_non_cases_pass_washout', 'U') IS NOT NULL DROP TABLE #temp_non_cases_pass_washout;


SELECT
    case_settings.characterization_case_id*10+2 as cohort_definition_id,
    t.row_id,
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
    -- Note: should this be >= or > ??
    AND o.cohort_end_date >= dateadd(day, -@outcome_washout, dateadd(day, @risk_window_start, t.@start_anchor_date))
    THEN 1 else 0 END) AS outcome_in_washout_before_tar,

    -- ADD has outcome in TAR (left join CASES on characterization_target_id, row_id and )
    MAX(CASE WHEN cases.row_id IS NOT NULL THEN 1 else 0 END) AS outcome_during_tar

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

    -- EDITED to collapse using outcome washout
     LEFT JOIN @characterization_schema.@outcome_era_table o
     --LEFT JOIN @cohort_schema.@cohort_table o

    ON t.subject_id = o.subject_id
    AND case_settings.outcome_id = o.cohort_definition_id
    AND o.outcome_washout = @outcome_washout

    -- outcome starts before TAR start
    AND o.cohort_start_date < dateadd(day, @risk_window_start, t.@start_anchor_date)
    -- outcome end after washout prior before TAR start
    AND o.cohort_end_date >= dateadd(day, -@outcome_washout, dateadd(day, @risk_window_start, t.@start_anchor_date))

    -- join to cases
    LEFT JOIN (SELECT * from @characterization_schema.@characterization_table
    WHERE char_type = 'cases' ) cases
    ON cases.cohort_definition_id = case_settings.characterization_case_id*10+1
    AND cases.row_id = t.row_id

    WHERE case_settings.outcome_id IN (@outcome_cohort_ids)
    AND case_settings.characterization_target_id IN (@characterization_target_ids)


    GROUP BY
    case_settings.characterization_case_id,
    t.row_id,
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
    cohort_definition_id, row_id, subject_id, cohort_start_date, cohort_end_date, char_type
  )

  SELECT
  temp.cohort_definition_id,
  temp.row_id,
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


-- add counts
-- add case count table
DELETE FROM @characterization_schema.@case_count_table
WHERE cohort_type = 'non-cases'
AND characterization_case_id in
(SELECT DISTINCT CAST((cohort_definition_id-2.0)/10.0 as BIGINT) FROM #temp_non_cases);

INSERT INTO @characterization_schema.@case_count_table(
characterization_case_id, cohort_type, n_events, n_people
)
SELECT
CAST((cohort_definition_id-2.0)/10.0 as BIGINT),
'non-cases',
count(*),
count(distinct subject_id)

FROM  #temp_non_cases temp

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

GROUP BY
cohort_definition_id
;




-- add the reasons for lost people due to tar/washout
SELECT *
INTO #temp_non_cases_with_tar
FROM #temp_non_cases temp

{@use_plp}?{
WHERE temp.no_tar_ignoring_outcome_washout = 0 AND temp.no_tar_obs = 0
}
{@use_ci}?{
WHERE temp.no_tar_ignoring_outcome_washout = 0 AND temp.no_tar_obs = 0
}
;

SELECT *
INTO #temp_non_cases_pass_washout
FROM #temp_non_cases_with_tar temp

{@use_plp}?{
WHERE temp.outcome_in_washout_before_tar = 0
}
{@use_ci}?{
WHERE temp.no_tar_because_outcome_washout = 0 AND temp.no_tar_washout_and_obs = 0
}
;


-- next
DELETE FROM @characterization_schema.@case_attrition_table
WHERE characterization_case_id in
(SELECT DISTINCT CAST((cohort_definition_id-2.0)/10.0 AS BIGINT) FROM #temp_non_cases);


INSERT INTO @characterization_schema.@case_attrition_table(
characterization_case_id, attr_order, attr_reason,
n_events, n_people
)

SELECT * FROM
(
SELECT
CAST((cohort_definition_id - 2.0)/10.0 AS BIGINT) as characterization_case_id,
8 as attr_order,
'Has some TAR' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people

FROM #temp_non_cases_with_tar
GROUP BY cohort_definition_id
) temp

-- add 0s
UNION
SELECT
CAST((cohort_definition_id - 2.0)/10.0 AS BIGINT),
8,
'Has some TAR',
0,
0
FROM #temp_non_cases
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_non_cases_with_tar)

;

INSERT INTO @characterization_schema.@case_attrition_table(
characterization_case_id, attr_order, attr_reason,
n_events, n_people
)

SELECT * FROM
(
SELECT
CAST((cohort_definition_id - 2.0)/10.0 AS BIGINT) as characterization_case_id,
9 as attr_order,
'Remains after outcome washout' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people

FROM #temp_non_cases_pass_washout
GROUP BY cohort_definition_id
) temp

UNION

SELECT
CAST((cohort_definition_id - 2.0)/10.0 AS BIGINT),
9,
'Remains after outcome washout',
0,
0
FROM #temp_non_cases_with_tar
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_non_cases_pass_washout)
;



-- cleaning table
IF OBJECT_ID('tempdb..#temp_non_cases', 'U') IS NOT NULL DROP TABLE #temp_non_cases;
IF OBJECT_ID('tempdb..#temp_non_cases_with_tar', 'U') IS NOT NULL DROP TABLE #temp_non_cases_with_tar;
IF OBJECT_ID('tempdb..#temp_non_cases_pass_washout', 'U') IS NOT NULL DROP TABLE #temp_non_cases_pass_washout;

