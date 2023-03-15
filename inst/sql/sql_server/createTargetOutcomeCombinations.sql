--need to know indication/target/outcome tuples
drop table if exists #targets_agg_all;
select * into #targets_agg_all
from @target_database_schema.@target_table
where cohort_definition_id in
(@target_ids);

-- first T with > minPrioObs
drop table if exists #targets_agg;
select * into #targets_agg
from
(select *,
row_number() over(partition by subject_id, cohort_definition_id, cohort_start_date order by cohort_start_date asc) as rn
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

drop table if exists #outcomes_agg;
select * into #outcomes_agg
from @outcome_database_schema.@outcome_table
where cohort_definition_id in
(@outcome_ids);

-- first outcomes
drop table if exists #outcomes_agg_first;
select * into #outcomes_agg_first
from (select *,
row_number() over(partition by subject_id, cohort_definition_id, cohort_start_date order by cohort_start_date asc) as rn
from #outcomes_agg
) as o
where o.rn = 1
;

-- create all the cohort details
drop table if exists #cohort_details;

select *,
ROW_NUMBER() OVER (ORDER BY cohort_type, target_cohort_id, outcome_cohort_id) as cohort_definition_id
into #cohort_details
from

(
select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TnO' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'OnT' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TnOc' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TnfirstO' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'firstOnT' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'TnfirstOc' as cohort_type
from
(select distinct cohort_definition_id from #targets_agg) as t
CROSS JOIN
(select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
t.cohort_definition_id as target_cohort_id,
0 as outcome_cohort_id,
'T' as cohort_type
from (select distinct cohort_definition_id from #targets_agg) as t

union

select distinct
t.cohort_definition_id as target_cohort_id,
0 as outcome_cohort_id,
'allT' as cohort_type
from (select distinct cohort_definition_id from #targets_agg) as t

union

select distinct
0 as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'O' as cohort_type
from (select distinct cohort_definition_id from #outcomes_agg) as o

union

select distinct
0 as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id,
'firstO' as cohort_type
from (select distinct cohort_definition_id from #outcomes_agg) as o


) temp;


-- 1) get all the people with the outcome in TAR
drop table if exists #target_with_outcome;

-- TnO
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_with_outcome
from #targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- outcome starts before TAR end
o.cohort_start_date <= dateadd(day, @tar_end, t.@tar_end_anchor)
and
-- outcome starts (ends?) after TAR start
o.cohort_start_date >= dateadd(day, @tar_start, t.@tar_start_anchor);

-- TnfirstO
drop table if exists #target_outcome_f;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_outcome_f
from #targets_agg t inner join #outcomes_agg_first o
on t.subject_id = o.subject_id
where
-- outcome starts before TAR end
o.cohort_start_date <= dateadd(day, @tar_end, t.@tar_end_anchor)
and
-- outcome starts (ends?) after TAR start
o.cohort_start_date >= dateadd(day, @tar_start, t.@tar_start_anchor);


-- 2) get all the people without the outcome in TAR
drop table if exists #target_nooutcome;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_nooutcome
from #targets_agg t
CROSS JOIN
( select distinct cohort_definition_id from #outcomes_agg) o
left outer join #target_with_outcome two
on t.cohort_definition_id = two.target_cohort_id
and t.subject_id = two.subject_id
and o.cohort_definition_id = two.outcome_cohort_id
where two.subject_id IS NULL;

drop table if exists #target_noout_f;
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #target_noout_f
from #targets_agg t
CROSS JOIN
( select distinct cohort_definition_id from #outcomes_agg) o
left outer join #target_outcome_f two
on t.cohort_definition_id = two.target_cohort_id
and t.subject_id = two.subject_id
and o.cohort_definition_id = two.outcome_cohort_id
where two.subject_id IS NULL;

-- Final: select into #agg_cohorts

select * into #agg_cohorts

from
(
-- T with O indexed at T

select
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #target_with_outcome tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'TnO'

union

-- T with first O indexed at T

select
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #target_outcome_f tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'TnfirstO'

union

-- T with O indexed at O

select
tno.subject_id,
tno.outcome_start_date as cohort_start_date,
tno.outcome_end_date as cohort_end_date,
cd.cohort_definition_id
from #target_with_outcome tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'OnT'

union

-- T with first O indexed at O

select
tno.subject_id,
tno.outcome_start_date as cohort_start_date,
tno.outcome_end_date as cohort_end_date,
cd.cohort_definition_id
from #target_outcome_f tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'firstOnT'


union

-- T without O

select
tnoc.subject_id,
tnoc.cohort_start_date,
tnoc.cohort_end_date,
cd.cohort_definition_id
from #target_nooutcome tnoc
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tnoc.target_cohort_id
and cd.outcome_cohort_id = tnoc.outcome_cohort_id
and cd.cohort_type = 'TnOc'

union

-- T without first O

select
tnoc.subject_id,
tnoc.cohort_start_date,
tnoc.cohort_end_date,
cd.cohort_definition_id
from #target_noout_f tnoc
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tnoc.target_cohort_id
and cd.outcome_cohort_id = tnoc.outcome_cohort_id
and cd.cohort_type = 'TnfirstOc'

union

-- Ts and Os

select distinct * from (

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_agg as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'T'

union

select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
cd.cohort_definition_id
from #targets_agg_all as t
INNER JOIN #cohort_details cd
on cd.target_cohort_id = t.cohort_definition_id
and cd.cohort_type = 'allT'

union

select
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_agg as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'O'

union

select
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_agg_first as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'firstO'

) temp_ts

) temp_ts2;
