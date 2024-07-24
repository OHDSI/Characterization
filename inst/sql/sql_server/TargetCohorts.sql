-- all targets
drop table if exists #targets_all;
select * into #targets_all
from @target_database_schema.@target_table
where cohort_definition_id in
(@target_ids);

-- first T with > minPrioObs
drop table if exists #targets_inclusions;
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


---- Create TAR agnostic cohorts
drop table if exists #agg_cohorts_before;
select * into #agg_cohorts_before

from
(

select distinct
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_inclusions as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'Target'


) temp_ts2;



-- add extra cohorts
drop table if exists #agg_cohorts_extras;
select * into #agg_cohorts_extras

from
(

select distinct
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_all as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'Tall'


) temp_ts2;
