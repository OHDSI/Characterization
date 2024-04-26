--need to know indication/target/outcome tuples

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

-- all outcomes
drop table if exists #outcomes_all;
select * into #outcomes_all
from @outcome_database_schema.@outcome_table
where cohort_definition_id in
(@outcome_ids);

-- first outcomes in washout days and min prior obs
drop table if exists #outcomes_washout;
select o.* into #outcomes_washout
from (select *,
ISNULL(datediff(day, LAG(cohort_start_date) OVER(partition by subject_id, cohort_definition_id order by cohort_start_date asc), cohort_start_date ), 100000) as time_between
from #outcomes_all
) as o
inner join @cdm_database_schema.observation_period op
on op.person_id = o.subject_id
and o.cohort_start_date >= op.observation_period_start_date
and o.cohort_start_date <= op.observation_period_end_date
where o.time_between >= @outcome_washout_days
and datediff(day, op.observation_period_start_date, o.cohort_start_date) >= @min_prior_observation;


-- 1) get all the people with the outcome in TAR
drop table if exists #target_outcome_tar;

-- cases
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_outcome_tar
from #targets_inclusions t inner join #outcomes_washout o
on t.subject_id = o.subject_id
where
-- outcome starts before TAR end
o.cohort_start_date <= dateadd(day, @tar_end, t.@tar_end_anchor)
and
-- outcome starts (ends?) after TAR start
o.cohort_start_date >= dateadd(day, @tar_start, t.@tar_start_anchor);


-- 2) get all the people with the outcome during washout
drop table if exists #target_outcome_prior;

-- people with outcome prior
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_outcome_prior
from #targets_inclusions t inner join #outcomes_washout o
on t.subject_id = o.subject_id
where
-- outcome starts and ends within prior_outcome_washout_days
o.cohort_start_date >= dateadd(day, -@outcome_washout_days, t.cohort_start_date)
and
o.cohort_start_date <= dateadd(day, -1, t.cohort_start_date);


drop table if exists #agg_cohorts_before;
select * into #agg_cohorts_before

from
(
-- cases indexed at T

select
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #target_outcome_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'TnO'

union

-- targets with O prior during washout

select
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #target_outcome_prior tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'TnOprior'

union

select distinct * from (

-- targets with restrictions

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_inclusions as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'T'

union

-- outcomes with restrictions
select
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_washout as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'O'

) temp_ts

) temp_ts2;


drop table if exists #agg_cohorts_after;
select * into #agg_cohorts_after

from
(
-- cases indexed at O
select
tno.subject_id,
tno.outcome_start_date as cohort_start_date,
tno.outcome_end_date as cohort_end_date,
cd.cohort_definition_id
from #target_outcome_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'OnT'

) temp_ts2;


drop table if exists #agg_cohorts_between;
select * into #agg_cohorts_between

from
(
-- cases with T start and O start as end
select
tno.subject_id,
tno.cohort_start_date,
tno.outcome_start_date as cohort_end_date,
cd.cohort_definition_id
from #target_outcome_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'TnObetween'

) temp_ts2;

drop table if exists #agg_cohorts_extras;
select * into #agg_cohorts_extras

from
(

select distinct * from (

-- targets with restrictions

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_inclusions as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'Tall'

union

-- outcomes with restrictions
select
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_washout as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'Oall'

) temp_ts

) temp_ts2;
