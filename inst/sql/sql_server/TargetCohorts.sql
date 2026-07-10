-- first entry in washout days and min prior obs

IF OBJECT_ID('tempdb..#temp_target', 'U') IS NOT NULL DROP TABLE #temp_target;
IF OBJECT_ID('tempdb..#temp_target_first', 'U') IS NOT NULL DROP TABLE #temp_target_first;
IF OBJECT_ID('tempdb..#temp_target_prior', 'U') IS NOT NULL DROP TABLE #temp_target_prior;
IF OBJECT_ID('tempdb..#temp_target_nest', 'U') IS NOT NULL DROP TABLE #temp_target_nest;
IF OBJECT_ID('tempdb..#temp_target_age', 'U') IS NOT NULL DROP TABLE #temp_target_age;
IF OBJECT_ID('tempdb..#temp_target_gender', 'U') IS NOT NULL DROP TABLE #temp_target_gender;
IF OBJECT_ID('tempdb..#temp_target_date', 'U') IS NOT NULL DROP TABLE #temp_target_date;

-- =========================
SELECT
CAST(target_settings.characterization_target_id AS BIGINT) AS cohort_definition_id,
row_number() over(PARTITION BY CAST(target_settings.characterization_target_id AS BIGINT) ORDER BY temp_cohort.subject_id, temp_cohort.cohort_start_date ASC) AS row_id,
temp_cohort.subject_id,
temp_cohort.cohort_start_date,
temp_cohort.cohort_end_date,
op.observation_period_start_date,
op.observation_period_end_date,
temp_cohort.time_between,
'target' as char_type

INTO #temp_target

FROM (SELECT
      cohort_definition_id,
      subject_id,
      cohort_start_date,
      cohort_end_date,
      -- edited this so it works even when cohort period are not ordered nicely
      ISNULL(DATEDIFF(day, MAX(cohort_end_date) OVER (PARTITION BY subject_id, cohort_definition_id ORDER BY cohort_start_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), cohort_start_date), (@limit_to_first_in_n_days+1)) AS time_between

      FROM @cohort_schema.@cohort_table WHERE cohort_definition_id IN (@cohort_ids)
) temp_cohort

INNER JOIN @cdm_database_schema.observation_period op
ON op.person_id = temp_cohort.subject_id
AND temp_cohort.cohort_start_date >= op.observation_period_start_date
AND temp_cohort.cohort_start_date <= op.observation_period_end_date

-- this is just to get the characterization_target_id
INNER JOIN
(SELECT distinct * FROM @target_settings_schema.@target_settings_table
 WHERE limit_to_first_in_n_days = @limit_to_first_in_n_days
 AND min_prior_observation = @min_prior_observation
 -- added:
 AND min_age = @min_age
 AND max_age = @max_age
 AND study_start = '@study_start'
 AND study_end = '@study_end'
 AND gender_concept_ids = '@gender_concept_ids'
 AND nesting_cohort_id = @nesting_cohort_id

 -- add target_ids?
 --AND target_id IN (@cohort_ids)

) target_settings
ON temp_cohort.cohort_definition_id = target_settings.target_id;


-- now do first in n
SELECT *
INTO #temp_target_first
FROM #temp_target temp_cohort
WHERE (temp_cohort.time_between >= @limit_to_first_in_n_days);

-- now min prior obs
SELECT *
INTO #temp_target_prior
FROM #temp_target_first
WHERE datediff(day, observation_period_start_date, cohort_start_date) >= @min_prior_observation;

-- now nesting
{@nesting_cohort_id != 0}?{SELECT
t.cohort_definition_id,
t.row_id,
t.subject_id,
t.cohort_start_date,
-- use the nesting end date if it is before the target end date
CASE WHEN t.cohort_end_date <= n.cohort_end_date THEN t.cohort_end_date
ELSE n.cohort_end_date END cohort_end_date,
t.observation_period_start_date,
t.observation_period_end_date,
t.char_type
INTO #temp_target_nest
FROM #temp_target_prior t
INNER JOIN
(SELECT * from @nesting_schema.@nesting_table
WHERE cohort_definition_id = @nesting_cohort_id) n
ON n.subject_id = t.subject_id
-- cohort starts between nesting date
AND n.cohort_start_date <= t.cohort_start_date
AND n.cohort_end_date >= t.cohort_start_date;
}:{
SELECT *
INTO #temp_target_nest
FROM #temp_target_prior t;
}

-- now age at start
SELECT *
INTO #temp_target_age
FROM #temp_target_nest t
INNER JOIN @cdm_database_schema.person p
ON p.person_id = t.subject_id
WHERE YEAR(t.cohort_start_date) - p.year_of_birth >= @min_age
AND YEAR(t.cohort_start_date) - p.year_of_birth <= @max_age;

-- now gender
{@gender_concept_ids != ''}?{
SELECT *
INTO #temp_target_gender
FROM #temp_target_age t
INNER JOIN
@cdm_database_schema.person p
ON p.person_id = t.subject_id
WHERE p.gender_concept_id = '@gender_concept_ids';
}:{
SELECT *
INTO #temp_target_gender
FROM #temp_target_age;
}

-- finally date:
{@study_start != '' | @study_end != ''}?{
SELECT
cohort_definition_id,
row_id,
subject_id,
cohort_start_date,
-- edit the end date if after study end
{@study_end != ''}?{
CASE WHEN @study_end < cohort_end_date THEN @study_end ELSE cohort_end_date END as cohort_end_date,
} :
{cohort_end_date,}
observation_period_start_date,
observation_period_end_date,
time_between,
char_type
INTO #temp_target_date
FROM #temp_target_gender
WHERE 1 = 1
{@study_start != ''}?{AND cohort_start_date >= CAST('@study_start' AS DATE)}
{@study_end != ''}?{AND cohort_start_date <= CAST('@study_end' AS DATE)}
;
} : {
SELECT *
INTO #temp_target_date
FROM #temp_target_gender;
}
-- =========================



-- =========================
-- ADDING FINAL COHORT INTO TABLE
-- remove existing rows with cohort ids
DELETE FROM @characterization_schema.@characterization_table
WHERE char_type = 'target'
AND cohort_definition_id in (SELECT DISTINCT cohort_definition_id FROM #temp_target_date)
;

-- insert the new rows
-- now determine the non-cases
  INSERT INTO @characterization_schema.@characterization_table(
    cohort_definition_id, row_id, subject_id, cohort_start_date, cohort_end_date,
    observation_period_start_date, observation_period_end_date, char_type
  )

  SELECT
  temp.cohort_definition_id,
  temp.row_id,
  temp.subject_id,
  temp.cohort_start_date,
  temp.cohort_end_date, -- TODO: update cohort_end_date to be study_end_date if study_end_date is before?
  temp.observation_period_start_date,
  temp.observation_period_end_date,
  'target' as char_type

  FROM #temp_target_date temp;
-- =========================



-- =========================
-- DO ATTRITION - how to get 0 when there are no rows?
DELETE FROM @characterization_schema.@target_attrition_table
WHERE characterization_target_id in (SELECT DISTINCT cohort_definition_id FROM #temp_target_date)
;

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people)

SELECT
cohort_definition_id,
1,
'Target Start',
count(*),
count(distinct subject_id)
FROM #temp_target
GROUP BY cohort_definition_id
;

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people)

SELECT * FROM
(SELECT
cohort_definition_id as characterization_target_id,
2 as attr_order,
'First in @limit_to_first_in_n_days days' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people
FROM #temp_target_first
GROUP BY cohort_definition_id) main

UNION

SELECT
cohort_definition_id,
2,
'First in @limit_to_first_in_n_days days',
0,
0
FROM #temp_target -- preious
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_target_first)

;

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people)

SELECT * FROM
(SELECT
cohort_definition_id as characterization_target_id,
3 as attr_order,
'With @min_prior_observation prior obs' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people
FROM #temp_target_prior
GROUP BY cohort_definition_id) main

UNION

SELECT
cohort_definition_id,
3,
'With @min_prior_observation prior obs',
0,
0
FROM #temp_target_first -- preious
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_target_prior)

;

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people)

SELECT * FROM
(SELECT
cohort_definition_id as characterization_target_id,
4 as attr_order,
'Nested in @nesting_cohort_id' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people
FROM #temp_target_nest
GROUP BY cohort_definition_id) main

UNION

SELECT
cohort_definition_id,
4,
'Nested in @nesting_cohort_id',
0,
0
FROM #temp_target_prior -- preious
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_target_nest)

;

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people)

SELECT * FROM
(SELECT
cohort_definition_id as characterization_target_id,
5 as attr_order,
'Aged @min_age to @max_age' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people
FROM #temp_target_age
GROUP BY cohort_definition_id) main

UNION

SELECT
cohort_definition_id,
5,
'Aged @min_age to @max_age',
0,
0
FROM #temp_target_nest -- preious
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_target_age)

;

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people)

SELECT * FROM
(SELECT
cohort_definition_id as characterization_target_id,
6 as attr_order,
'Gender in @gender_concept_ids' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people
FROM #temp_target_gender
GROUP BY cohort_definition_id) main

UNION

SELECT
cohort_definition_id,
6,
'Gender in @gender_concept_ids',
0,
0
FROM #temp_target_age -- preious
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_target_gender);

INSERT INTO @characterization_schema.@target_attrition_table(
characterization_target_id, attr_order, attr_reason,
n_events, n_people
)

SELECT * FROM
(SELECT
cohort_definition_id as characterization_target_id,
7 as attr_order,
'Starting between @study_start to @study_end' as attr_reason,
count(*) as n_events,
count(distinct subject_id) as n_people

FROM #temp_target_date
GROUP BY cohort_definition_id) main

UNION

SELECT
cohort_definition_id,
7,
'Starting between @study_start to @study_end',
0,
0
FROM #temp_target_gender
WHERE cohort_definition_id NOT IN
(SELECT distinct cohort_definition_id FROM #temp_target_date)

;

-- =========================


-- add final target count to table
DELETE FROM @characterization_schema.@target_count_table
WHERE characterization_target_id in (SELECT DISTINCT cohort_definition_id FROM #temp_target_date)
;

INSERT INTO @characterization_schema.@target_count_table
SELECT
cohort_definition_id as characterization_target_id,
count(*) as n_events, -- new
count(distinct subject_id) as n_people -- new

FROM #temp_target_date

GROUP BY
cohort_definition_id
;

-- =========================
-- clean up
IF OBJECT_ID('tempdb..#temp_target', 'U') IS NOT NULL DROP TABLE #temp_target;
IF OBJECT_ID('tempdb..#temp_target_first', 'U') IS NOT NULL DROP TABLE #temp_target_first;
IF OBJECT_ID('tempdb..#temp_target_prior', 'U') IS NOT NULL DROP TABLE #temp_target_prior;
IF OBJECT_ID('tempdb..#temp_target_nest', 'U') IS NOT NULL DROP TABLE #temp_target_nest;
IF OBJECT_ID('tempdb..#temp_target_age', 'U') IS NOT NULL DROP TABLE #temp_target_age;
IF OBJECT_ID('tempdb..#temp_target_gender', 'U') IS NOT NULL DROP TABLE #temp_target_gender;
IF OBJECT_ID('tempdb..#temp_target_date', 'U') IS NOT NULL DROP TABLE #temp_target_date;
-- =========================




