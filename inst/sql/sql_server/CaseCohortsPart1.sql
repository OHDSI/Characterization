--need to know indication/target/outcome tuples

-- all targets
IF OBJECT_ID('tempdb..#targets_all', 'U') IS NOT NULL DROP TABLE #targets_all;
select * into #targets_all
from @target_database_schema.@target_table
where cohort_definition_id in
(@target_ids);

-- first T with > minPrioObs
IF OBJECT_ID('tempdb..#targets_inclusions', 'U') IS NOT NULL DROP TABLE #targets_inclusions;
select * into #targets_inclusions
from
(select *,
  row_number() over(partition by subject_id, cohort_definition_id order by cohort_start_date asc) as rn
  from @target_database_schema.@target_table
  where cohort_definition_id in
  (@target_ids)
) temp_t
inner join @cdm_database_schema.observation_period op
on op.person_id = temp_t.subject_id
and temp_t.cohort_start_date >= op.observation_period_start_date
and temp_t.cohort_start_date <= op.observation_period_end_date
where temp_t.rn = 1
and datediff(day, op.observation_period_start_date, temp_t.cohort_start_date) >= @min_prior_observation;

-- all outcomes
IF OBJECT_ID('tempdb..#outcomes_all', 'U') IS NOT NULL DROP TABLE #outcomes_all;
select * into #outcomes_all
from @outcome_database_schema.@outcome_table
where cohort_definition_id in
(@outcome_ids);

-- first outcomes in washout days and min prior obs
IF OBJECT_ID('tempdb..#outcomes_washout', 'U') IS NOT NULL DROP TABLE #outcomes_washout;
select o.* into #outcomes_washout
from (select *,
      ISNULL(datediff(day, LAG(cohort_start_date) OVER(partition by subject_id, cohort_definition_id order by cohort_start_date asc), cohort_start_date ), 100000) as time_between
      from #outcomes_all
) o
inner join @cdm_database_schema.observation_period op
on op.person_id = o.subject_id
and o.cohort_start_date >= op.observation_period_start_date
and o.cohort_start_date <= op.observation_period_end_date
where o.time_between >= @outcome_washout_days
and datediff(day, op.observation_period_start_date, o.cohort_start_date) >= @min_prior_observation;


-- 2) get all the people with the outcome during washout
IF OBJECT_ID('tempdb..#case_exclude', 'U') IS NOT NULL DROP TABLE #case_exclude;

-- people with outcome prior
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #case_exclude
from #targets_inclusions t inner join #outcomes_washout o
on t.subject_id = o.subject_id
where
-- outcome starts and ends within prior_outcome_washout_days
o.cohort_start_date >= dateadd(day, -@outcome_washout_days, t.cohort_start_date)
and
o.cohort_start_date <= dateadd(day, -1, t.cohort_start_date);



---- Create TAR agnostic cohorts
IF OBJECT_ID('tempdb..#cases', 'U') IS NOT NULL DROP TABLE #cases;
select * into #cases

from
(

-- targets with O prior during washout
select distinct
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #case_exclude tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'Exclude' -- changed from TnOprior

) temp_ts2;

-- drop the table needed by the case series cohorts
IF OBJECT_ID('tempdb..#case_series', 'U') IS NOT NULL DROP TABLE #case_series;

