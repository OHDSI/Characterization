-- PER TAR RUN TO GET TnO cohorts

-- 1) get all the people with the outcome in TAR
IF OBJECT_ID('tempdb..#cases_tar', 'U') IS NOT NULL DROP TABLE #cases_tar;
-- cases
select
t.subject_id,
t.cohort_start_date,
t.cohort_end_date,
o.cohort_start_date as outcome_start_date,
o.cohort_end_date as outcome_end_date,
t.cohort_definition_id as target_cohort_id,
o.cohort_definition_id as outcome_cohort_id
into #cases_tar
from #targets_inclusions t inner join #outcomes_washout o
on t.subject_id = o.subject_id
where
-- outcome starts before TAR end
o.cohort_start_date <= dateadd(day, @tar_end, t.@tar_end_anchor)
and
-- outcome starts (ends?) after TAR start
o.cohort_start_date >= dateadd(day, @tar_start, t.@tar_start_anchor);

-- add the cases for specific TAR
insert into #cases
select *
from
(
-- cases indexed at T

select
tno.subject_id,
tno.cohort_start_date,
tno.cohort_end_date,
cd.cohort_definition_id
from #cases_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'Cases'
and cd.setting_id = '@setting_id'

) temp_ts2;

{@first}?{select * into #case_series}:{insert into #case_series select *}

from
(
-- cases with T start and O start as end
select
tno.subject_id,
dateadd(day, 1, tno.cohort_start_date) as cohort_start_date,
tno.outcome_start_date as cohort_end_date,
cd.cohort_definition_id
from #cases_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'CasesBetween'
and cd.setting_id = '@setting_id'

union

-- cases indexed at O
select
tno.subject_id,
dateadd(day, 1, tno.outcome_start_date) as cohort_start_date,
dateadd(day, @case_post_outcome_duration, tno.outcome_start_date) as cohort_end_date,
cd.cohort_definition_id
from #cases_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'CasesAfter'
and cd.setting_id = '@setting_id'

union

select
tno.subject_id,
dateadd(day, -@case_pre_target_duration, tno.cohort_start_date) as cohort_start_date,
tno.cohort_start_date as cohort_end_date,
cd.cohort_definition_id
from #cases_tar tno
INNER JOIN #cohort_details cd
on cd.target_cohort_id = tno.target_cohort_id
and cd.outcome_cohort_id = tno.outcome_cohort_id
and cd.cohort_type = 'CasesBefore'
and cd.setting_id = '@setting_id'

) temp_ts2;


