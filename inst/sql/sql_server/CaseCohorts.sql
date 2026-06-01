-- PER Setting (newt, outcome, outcome washout, tar)

-- 1) get all the people with the outcome in TAR
IF OBJECT_ID('tempdb..#characterization_cases', 'U') IS NOT NULL DROP TABLE #characterization_cases;
-- cases (first outcome date)

SELECT
case_settings.characterization_case_id as cohort_definition_id,
t.row_number,
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
MIN(o.cohort_start_date) AS outcome_start_date,
MIN(o.cohort_end_date) AS outcome_end_date,
t.cohort_definition_id AS characterization_target_id,
o.cohort_definition_id AS outcome_cohort_id,
t.observation_period_start_date,
t.observation_period_end_date

INTO #characterization_cases

FROM @characterization_schema.@characterization_table t

INNER JOIN
( SELECT *,
  ISNULL(datediff(day, LAG(cohort_end_date) OVER(partition BY subject_id, cohort_definition_id ORDER BY cohort_start_date ASC), cohort_start_date ), (@outcome_washout+1)) outcome_washout_time
  FROM @cohort_schema.@cohort_table
  WHERE cohort_definition_id IN (@outcome_cohort_ids)
) o
ON t.subject_id = o.subject_id

INNER JOIN
(SELECT * FROM @case_settings_schema.@case_settings_table
 WHERE outcome_washout_days = @outcome_washout
 AND risk_window_start = @risk_window_start
 AND risk_window_end = @risk_window_end
 AND start_anchor = '@start_anchor'
 AND end_anchor = '@end_anchor'
) case_settings
ON t.cohort_definition_id = case_settings.characterization_target_id
AND o.cohort_definition_id = case_settings.outcome_id

WHERE case_settings.characterization_target_id IN (@characterization_target_ids)
AND o.outcome_washout_time > @outcome_washout
AND o.cohort_start_date >= t.observation_period_start_date
AND o.cohort_start_date <= t.observation_period_end_date
-- outcome starts before TAR end
AND o.cohort_start_date <= dateadd(day, @risk_window_end, t.@end_anchor_date)
-- outcome starts (ends?) after TAR start
AND o.cohort_start_date >= dateadd(day, @risk_window_start, t.@start_anchor_date)
-- make sure to only get first outcome date during TAR

GROUP BY
case_settings.characterization_case_id,
t.row_number,
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id,
o.cohort_definition_id,
t.observation_period_start_date,
t.observation_period_end_date
;


{@include_risk_factor}?{
DELETE FROM @characterization_schema.@characterization_table
WHERE char_type = 'cases'
AND cohort_definition_id in (SELECT DISTINCT cohort_definition_id*10+1 FROM #characterization_cases);

INSERT INTO @characterization_schema.@characterization_table(
cohort_definition_id, row_number, subject_id, cohort_start_date, cohort_end_date, char_type
)
SELECT
cohort_definition_id*10+1,
row_number,
subject_id,
cohort_start_date,
cohort_end_date,
'cases'
FROM #characterization_cases;
}

{@include_case_series}?{
DELETE FROM @characterization_schema.@characterization_table
WHERE char_type = 'case-series'
AND cohort_definition_id in
(
SELECT cohort_definition_id*10+3 FROM #characterization_cases
UNION
SELECT cohort_definition_id*10+4 FROM #characterization_cases
UNION
SELECT cohort_definition_id*10+5 FROM #characterization_cases
);


INSERT INTO @characterization_schema.@characterization_table(
cohort_definition_id, row_number, subject_id, cohort_start_date, cohort_end_date, char_type
)
SELECT
cohort_definition_id*10+3,
row_number,
subject_id,
DATEADD(day, -@case_series_before, cohort_start_date),
DATEADD(day, 0, cohort_start_date),
'case-series'
FROM #characterization_cases


UNION

SELECT
cohort_definition_id*10+4,
row_number,
subject_id,
DATEADD(day, 1, cohort_start_date),
DATEADD(day, 0, outcome_start_date),
'case-series'
FROM #characterization_cases

UNION

SELECT
cohort_definition_id*10+5,
row_number,
subject_id,
DATEADD(day, 1, outcome_start_date),
DATEADD(day, @case_series_after, outcome_end_date),
'case-series'
FROM #characterization_cases;

}

-- add case count table
DELETE FROM @characterization_schema.@case_count_table
WHERE cohort_type = 'Cases'
AND characterization_case_id in
(SELECT DISTINCT cohort_definition_id FROM #characterization_cases);

INSERT INTO @characterization_schema.@case_count_table
SELECT
cohort_definition_id as characterization_case_id,
'Cases' as cohort_type,
count(*) as n_events, -- new
count(distinct subject_id) as n_people -- new

FROM #characterization_cases

GROUP BY
cohort_definition_id
;

-- clean up
IF OBJECT_ID('tempdb..#characterization_cases', 'U') IS NOT NULL DROP TABLE #characterization_cases;


