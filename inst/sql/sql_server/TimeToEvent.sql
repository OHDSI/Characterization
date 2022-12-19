--need to know indication/target/outcome tuples
drop table if exists #targets;
create table #targets as
select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date asc) as era_number
from @target_database_schema.@target_table
where cohort_definition_id in
(select distinct target_cohort_definition_id from #cohort_settings)
;

drop table if exists #outcomes;
create table #outcomes as
select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date asc) as era_number
from @outcome_database_schema.@outcome_table
where cohort_definition_id in (select distinct outcome_cohort_definition_id from #cohort_settings)
;


drop table if exists #target_w_outcome;
create table #target_w_outcome as
select t1.cohort_definition_id as target_cohort_definition_id, o1.cohort_definition_id as outcome_cohort_definition_id,
      t1.subject_id, t1.first_start_date as target_first_start_date, t1.last_end_date as target_last_end_date, t1.num_eras as target_num_eras, t1.total_duration as target_total_duration,
      o1.first_start_date as outcome_first_start_date, o1.last_end_date as outcome_last_end_date, o1.num_eras as outcome_num_eras, o1.total_duration as outcome_total_duration
from (
  select cohort_definition_id, subject_id,
        min(cohort_start_date) as first_start_date,
        max(cohort_end_date) as last_end_date,
        count(subject_id) as num_eras,
        sum(datediff(day, cohort_start_date, cohort_end_date)) as total_duration
  from #targets
  group by cohort_definition_id, subject_id
  ) t1
inner join (
  select cohort_definition_id, subject_id,
        min(cohort_start_date) as first_start_date,
        max(cohort_end_date) as last_end_date,
        count(subject_id) as num_eras,
        sum(datediff(day, cohort_start_date, cohort_end_date)) as total_duration
  from #outcomes
  group by cohort_definition_id, subject_id
  ) o1
on t1.subject_id = o1.subject_id
inner join #cohort_settings ito1
on t1.cohort_definition_id = ito1.target_cohort_definition_id
and o1.cohort_definition_id = ito1.outcome_cohort_definition_id
;


/*follow-up time*/
drop table if exists #two_fu_bounds;
create table #two_fu_bounds as
select
  t1.cohort_definition_id,
  min(datediff(day,t1.first_date, op1.observation_period_start_date)) as min_time,
  max(datediff(day,t1.first_date, op1.observation_period_end_date)) as max_time
from (
  select cohort_definition_id, subject_id, min(cohort_start_date) as first_date
  from #targets
  group by cohort_definition_id, subject_id
  ) t1
inner join @cdm_database_schema.observation_period op1
on t1.subject_id = op1.person_id
and t1.first_date >= op1.observation_period_start_date
and t1.first_date <= op1.observation_period_end_date
group by
  t1.cohort_definition_id
;

--select * from #two_fu_bounds;


drop table if exists #t_prior_obs;
create table #t_prior_obs as
select
  t1.cohort_definition_id,
  'Before target start' as observation_time_type,
  datediff(day, t1.first_date, op1.observation_period_start_date) as time_to_event,
  count(t1.subject_id) as num_persons
from (
  select cohort_definition_id, subject_id, min(cohort_start_date) as first_date
  from #targets
  group by cohort_definition_id, subject_id
  ) t1
inner join @cdm_database_schema.observation_period op1
on t1.subject_id = op1.person_id
and t1.first_date >= op1.observation_period_start_date
and t1.first_date <= op1.observation_period_end_date
group by
  t1.cohort_definition_id,
  datediff(day, t1.first_date, op1.observation_period_start_date)
;


drop table if exists #t_post_obs;
create table #t_post_obs as
select
  t1.cohort_definition_id,
  'After target start' as observation_time_type,
  datediff(day, t1.first_date, op1.observation_period_end_date) as time_to_event,
  count(t1.subject_id) as num_persons
from (
  select cohort_definition_id, subject_id, min(cohort_start_date) as first_date
  from #targets
  group by cohort_definition_id, subject_id
  ) t1
inner join @cdm_database_schema.observation_period op1
on t1.subject_id = op1.person_id
and t1.first_date >= op1.observation_period_start_date
and t1.first_date <= op1.observation_period_end_date
group by
  t1.cohort_definition_id,
  datediff(day, t1.first_date, op1.observation_period_end_date)
;


/*time-to-event distribution*/
drop table if exists #two_tte;
create table #two_tte as
select two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end as outcome_type,
  'Before first target start' as target_outcome_type,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date) as time_to_event,
  count(o1.subject_id) as num_events
from #target_w_outcome two1
inner join @cdm_database_schema.observation_period op1
on two1.subject_id = op1.person_id
and two1.target_first_start_date >= op1.observation_period_start_date
and two1.target_first_start_date <= op1.observation_period_end_date
inner join #outcomes o1
on two1.outcome_cohort_definition_id = o1.cohort_definition_id
and two1.subject_id = o1.subject_id
and two1.target_first_start_date > o1.cohort_start_date
and o1.cohort_start_date >= op1.observation_period_start_date
and o1.cohort_start_date <= op1.observation_period_end_date
group by two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date)

union all

select two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end as outcome_type,
  'During first' as target_outcome_type,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date) as time_to_event,
  count(o1.subject_id) as num_events
from #target_w_outcome two1
inner join @cdm_database_schema.observation_period op1
on two1.subject_id = op1.person_id
and two1.target_first_start_date >= op1.observation_period_start_date
and two1.target_first_start_date <= op1.observation_period_end_date
inner join (select * from #targets where era_number=1) t1
on two1.target_cohort_definition_id = t1.cohort_definition_id
and two1.subject_id = t1.subject_id
inner join #outcomes o1
on two1.outcome_cohort_definition_id = o1.cohort_definition_id
and two1.subject_id = o1.subject_id
and two1.target_first_start_date <= o1.cohort_start_date
and t1.cohort_start_date <= o1.cohort_start_date
and t1.cohort_end_date >= o1.cohort_start_date
and o1.cohort_start_date >= op1.observation_period_start_date
and o1.cohort_start_date <= op1.observation_period_end_date
group by two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date)

union all

select two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end as outcome_type,
  'During subsequent' as target_outcome_type,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date) as time_to_event,
  count(o1.subject_id) as num_events
from #target_w_outcome two1
inner join @cdm_database_schema.observation_period op1
on two1.subject_id = op1.person_id
and two1.target_first_start_date >= op1.observation_period_start_date
and two1.target_first_start_date <= op1.observation_period_end_date
inner join (select * from #targets where era_number>1) t1
on two1.target_cohort_definition_id = t1.cohort_definition_id
and two1.subject_id = t1.subject_id
inner join #outcomes o1
on two1.outcome_cohort_definition_id = o1.cohort_definition_id
and two1.subject_id = o1.subject_id
and two1.target_first_start_date <= o1.cohort_start_date
and t1.cohort_start_date <= o1.cohort_start_date
and t1.cohort_end_date >= o1.cohort_start_date
and o1.cohort_start_date >= op1.observation_period_start_date
and o1.cohort_start_date <= op1.observation_period_end_date
group by two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date)

union all

select two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end as outcome_type,
  'Between target eras' as target_outcome_type,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date) as time_to_event,
  count(o1.subject_id) as num_events
from #target_w_outcome two1
inner join @cdm_database_schema.observation_period op1
on two1.subject_id = op1.person_id
and two1.target_first_start_date >= op1.observation_period_start_date
and two1.target_first_start_date <= op1.observation_period_end_date
inner join #outcomes o1
on two1.outcome_cohort_definition_id = o1.cohort_definition_id
and two1.subject_id = o1.subject_id
and two1.target_first_start_date <= o1.cohort_start_date
and two1.target_last_end_date >= o1.cohort_start_date
and o1.cohort_start_date >= op1.observation_period_start_date
and o1.cohort_start_date <= op1.observation_period_end_date
left join #targets t1
on two1.target_cohort_definition_id = t1.cohort_definition_id
and two1.subject_id = t1.subject_id
and t1.cohort_start_date <= o1.cohort_start_date
and t1.cohort_end_date >= o1.cohort_start_date
where t1.subject_id is null
group by two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date)

union all

select two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end as outcome_type,
  'After last target end' as target_outcome_type,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date) as time_to_event,
  count(o1.subject_id) as num_events
from #target_w_outcome two1
inner join @cdm_database_schema.observation_period op1
on two1.subject_id = op1.person_id
and two1.target_first_start_date >= op1.observation_period_start_date
and two1.target_first_start_date <= op1.observation_period_end_date
inner join #outcomes o1
on two1.outcome_cohort_definition_id = o1.cohort_definition_id
and two1.subject_id = o1.subject_id
and two1.target_last_end_date < o1.cohort_start_date
and o1.cohort_start_date >= op1.observation_period_start_date
and o1.cohort_start_date <= op1.observation_period_end_date
group by two1.target_cohort_definition_id, two1.outcome_cohort_definition_id,
  case when o1.era_number = 1 then 'first' else 'subsequent' end,
  datediff(day, two1.target_first_start_date, o1.cohort_start_date)
;


drop table if exists #two_tte_summary;
create table #two_tte_summary as
select '@database_id' as database_id, temp.* from
(
--daily counting for +/- 100 days
select target_cohort_definition_id, outcome_cohort_definition_id, outcome_type, target_outcome_type, time_to_event, num_events, 'per 1-day' as time_scale
from #two_tte
where abs(time_to_event) <= 100

union all

--30-day counting for +/- 1080 days (~ 3 years)
select target_cohort_definition_id, outcome_cohort_definition_id, outcome_type, target_outcome_type,
  case when time_to_event = 0 then 0
       when time_to_event < 0 then (floor(time_to_event/30)-1)*30
       when time_to_event > 0 then (floor(time_to_event/30)+1)*30 end as time_to_event, sum(num_events) as num_events, 'per 30-day' as time_scale
from #two_tte
where abs(time_to_event) <= 1080
group by target_cohort_definition_id, outcome_cohort_definition_id, outcome_type, target_outcome_type,
  case when time_to_event = 0 then 0
       when time_to_event < 0 then (floor(time_to_event/30)-1)*30
       when time_to_event > 0 then (floor(time_to_event/30)+1)*30 end

union all

--365-day counting for +/- all days
select target_cohort_definition_id, outcome_cohort_definition_id, outcome_type, target_outcome_type,
  case when time_to_event = 0 then 0
       when time_to_event < 0 then (floor(time_to_event/365)-1)*365
       when time_to_event > 0 then (floor(time_to_event/365)+1)*365 end as time_to_event, sum(num_events) as num_events, 'per 365-day' as time_scale
from #two_tte
group by target_cohort_definition_id, outcome_cohort_definition_id, outcome_type, target_outcome_type,
  case when time_to_event = 0 then 0
       when time_to_event < 0 then (floor(time_to_event/365)-1)*365
       when time_to_event > 0 then (floor(time_to_event/365)+1)*365 end

) temp
;

-- select * from #two_tte_summary;








