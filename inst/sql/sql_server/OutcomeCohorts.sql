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


---- Create TAR agnostic cohorts
drop table if exists #agg_cohorts_before;
select * into #agg_cohorts_before

from
(
-- outcomes with restrictions
select distinct
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_washout as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'Outcome'

) temp_ts2;



-- add extra cohorts
drop table if exists #agg_cohorts_extras;
select * into #agg_cohorts_extras

from
(
-- outcomes with restrictions
select distinct
o.subject_id,
o.cohort_start_date,
o.cohort_end_date,
cd.cohort_definition_id
from #outcomes_washout as o
INNER JOIN #cohort_details cd
on cd.outcome_cohort_id = o.cohort_definition_id
and cd.cohort_type = 'Oall'

) temp_ts2;
