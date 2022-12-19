--need to know indication/target/outcome tuples
select * into #targets_agg
from @target_database_schema.@target_table
where cohort_definition_id in
(@target_ids);

select * into #outcomes_agg
from @outcome_database_schema.@outcome_table
where cohort_definition_id in
(@outcome_ids);


-- 1) get all the people with the outcome in TAR
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_id,
o.cohort_definition_id as outcome_id,
'TnO' as cohort_type,
t.cohort_definition_id*100000 +
o.cohort_definition_id*10 +  -- use row_number of o?
1 as cohort_definition_id
into #target_with_outcome
from
#targets_agg t inner join #outcomes_agg o
on t.subject_id = o.subject_id
where
-- outcome starts before TAR end
o.cohort_start_date <= dateadd(day, @tar_end, t.@tar_end_anchor)
and
-- outcome starts (ends?) after TAR start
o.cohort_start_date >= dateadd(day, @tar_start, t.@tar_start_anchor);


-- 2) get all the people without the outcome in TAR
drop table if exists #target_nooutcome;
create table #target_nooutcome as
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
t.cohort_definition_id as target_id,
o.outcome_id as outcome_id,
'TnOc' as cohort_type,
t.cohort_definition_id*100000 +
o.outcome_id*10 +
2 as cohort_definition_id
from
#targets_agg t
CROSS JOIN
( select distinct cohort_definition_id as outcome_id from #outcomes_agg) o
left outer join #target_with_outcome two
on t.cohort_definition_id = two.target_id
and t.subject_id = two.subject_id
and o.outcome_id = two.outcome_id
where two.subject_id IS NULL;

-- Final: select into #agg_cohorts

select * into #agg_cohorts

from
(
-- T with O indexed at T

select
subject_id,
cohort_start_date,
cohort_end_date,
cohort_definition_id -- ends in 1
from #target_with_outcome

union

-- T with O indexed at O

select
subject_id,
outcome_start_date as cohort_start_date,
outcome_end_date as cohort_end_date,
cohort_definition_id + 2 -- making it end in 3
from #target_with_outcome

union

-- T without O

select
subject_id,
cohort_start_date,
cohort_end_date,
cohort_definition_id  -- ends in 2
from #target_nooutcome

union

-- Ts and Os

select distinct * from (

select
subject_id,
cohort_start_date,
cohort_end_date,
cohort_definition_id*100000
from #targets_agg

union

select
subject_id,
cohort_start_date,
cohort_end_date,
cohort_definition_id*100000
from #outcomes_agg
) temp_ts

) temp_ts2;
