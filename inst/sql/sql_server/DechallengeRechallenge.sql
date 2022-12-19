select * into #target_cohort
from @target_database_schema.@target_table
where cohort_definition_id in (@target_ids)
;

select * into #outcome_cohort
from @outcome_database_schema.@outcome_table
where cohort_definition_id in (@outcome_ids)
;

select
'@database_id' as database_id,
@dechallenge_stop_interval as dechallenge_stop_interval,
@dechallenge_evaluation_window as dechallenge_evaluation_window,
target_cohort_definition_id,
	outcome_cohort_definition_id,
	num_exposure_eras,
	num_persons_exposed,
	num_cases,
	dechallenge_attempt,
	dechallenge_fail,
	dechallenge_success,
	rechallenge_attempt,
	rechallenge_fail,
	rechallenge_success,
	case when num_cases > 0 then 1.0*dechallenge_attempt / num_cases else null end as pct_dechallenge_attempt,
	case when dechallenge_attempt > 0 then 1.0*dechallenge_success / dechallenge_attempt else null end as pct_dechallenge_success,
	case when dechallenge_attempt > 0 then 1.0*dechallenge_fail / dechallenge_attempt else null end as pct_dechallenge_fail,
	case when dechallenge_attempt > 0 then 1.0*rechallenge_attempt / dechallenge_attempt else null end as pct_rechallenge_attempt,
	case when rechallenge_attempt > 0 then 1.0*rechallenge_success / rechallenge_attempt else null end as pct_rechallenge_success,
	case when rechallenge_attempt > 0 then 1.0*rechallenge_fail / rechallenge_attempt else null end as pct_rechallenge_fail

	into #challenge
from
(
	select cases.target_cohort_definition_id, cases.outcome_cohort_definition_id,
		exposures.num_exposure_eras,
		exposures.num_persons_exposed,
    cases.num_cases,
		case when dechallenge_attempt.num_cases_dechallenge_attempt is not null then dechallenge_attempt.num_cases_dechallenge_attempt else 0 end as dechallenge_attempt,
		case when dechallenge_fail.num_cases_dechallenge_fail is not null then dechallenge_fail.num_cases_dechallenge_fail else 0 end as dechallenge_fail,
		case when dechallenge_attempt.num_cases_dechallenge_attempt is not null then dechallenge_attempt.num_cases_dechallenge_attempt else 0 end - case when dechallenge_fail.num_cases_dechallenge_fail is not null then dechallenge_fail.num_cases_dechallenge_fail else 0 end as dechallenge_success,
		case when rechallenge_attempt.num_cases_rechallenge is not null then rechallenge_attempt.num_cases_rechallenge else 0 end as rechallenge_attempt,
		case when rechallenge_fail.num_cases_rechallenge is not null then rechallenge_fail.num_cases_rechallenge else 0 end as rechallenge_fail,
		case when rechallenge_attempt.num_cases_rechallenge is not null then rechallenge_attempt.num_cases_rechallenge else 0 end - case when rechallenge_fail.num_cases_rechallenge is not null then rechallenge_fail.num_cases_rechallenge else 0 end as rechallenge_success

from
	(
	   select cohort_definition_id, count(subject_id) as num_exposure_eras, count(distinct subject_id) as num_persons_exposed
	   from #target_cohort
	   group by cohort_definition_id
	) exposures
	inner join
  (
	select dc1.cohort_definition_id as target_cohort_definition_id, io1.cohort_definition_id as outcome_cohort_definition_id, count(dc1.subject_id) as num_cases
	from #target_cohort dc1
	inner join #outcome_cohort io1
	on dc1.subject_id = io1.subject_id
	and io1.cohort_start_date > dc1.cohort_start_date  and io1.cohort_start_date <= dc1.cohort_end_date
	group by dc1.cohort_definition_id, io1.cohort_definition_id
	) cases
	on exposures.cohort_definition_id = cases.target_cohort_definition_id
	left join
	(
	select dc1.cohort_definition_id as target_cohort_definition_id, io1.cohort_definition_id as outcome_cohort_definition_id, count(distinct dc1.subject_id) as num_cases_dechallenge_attempt
	from #target_cohort dc1
	inner join #outcome_cohort io1
	on dc1.subject_id = io1.subject_id
	and io1.cohort_start_date > dc1.cohort_start_date and io1.cohort_start_date <= dc1.cohort_end_date
	and dc1.cohort_end_date <= dateadd(day,@dechallenge_stop_interval,io1.cohort_start_date)
	group by dc1.cohort_definition_id, io1.cohort_definition_id
	) dechallenge_attempt
	on cases.target_cohort_definition_id = dechallenge_attempt.target_cohort_definition_id
	and cases.outcome_cohort_definition_id = dechallenge_attempt.outcome_cohort_definition_id
	left join
	(
	select dc1.cohort_definition_id as target_cohort_definition_id, io1.cohort_definition_id as outcome_cohort_definition_id, count(distinct dc1.subject_id) as num_cases_dechallenge_fail
	from #target_cohort dc1
	inner join #outcome_cohort io1
	on dc1.subject_id = io1.subject_id
	and io1.cohort_start_date > dc1.cohort_start_date and io1.cohort_start_date <= dc1.cohort_end_date
	and dc1.cohort_end_date <= dateadd(day,@dechallenge_stop_interval,io1.cohort_start_date)
	inner join #outcome_cohort ro1
	on dc1.subject_id = ro1.subject_id
	and io1.cohort_definition_id = ro1.cohort_definition_id
	and ro1.cohort_start_date > dc1.cohort_end_date
	and ro1.cohort_start_date <= dateadd(day, @dechallenge_evaluation_window, dc1.cohort_end_date)   --this should be parameterized to be the dechallenge window required for success/failure
	group by dc1.cohort_definition_id, io1.cohort_definition_id
	) dechallenge_fail
	on cases.target_cohort_definition_id = dechallenge_fail.target_cohort_definition_id
	and cases.outcome_cohort_definition_id = dechallenge_fail.outcome_cohort_definition_id
	left join
	(
	select dc1.cohort_definition_id as target_cohort_definition_id, io1.cohort_definition_id as outcome_cohort_definition_id, count(distinct dc1.subject_id) as num_cases_rechallenge
	from #target_cohort dc1
	inner join #outcome_cohort  io1
	on dc1.subject_id = io1.subject_id
	and io1.cohort_start_date > dc1.cohort_start_date and io1.cohort_start_date <= dc1.cohort_end_date
	and dc1.cohort_end_date <= dateadd(day,@dechallenge_stop_interval,io1.cohort_start_date)
	left join #outcome_cohort ro0
	on dc1.subject_id = ro0.subject_id
	and io1.cohort_definition_id = ro0.cohort_definition_id
	and ro0.cohort_start_date > dc1.cohort_end_date
	and ro0.cohort_start_date <= dateadd(day, @dechallenge_evaluation_window, dc1.cohort_end_date)   --this should be parameterized to be the dechallenge window required for success/failure
	inner join #target_cohort de1
	on dc1.subject_id = de1.subject_id
	and dc1.cohort_definition_id = de1.cohort_definition_id
	and de1.cohort_start_date > dateadd(day, @dechallenge_evaluation_window, dc1.cohort_end_date)   --using same dechallenge window to detrmine when rechallenge attempt can start
	where ro0.subject_id is null --not a dechallenge fail
	group by dc1.cohort_definition_id, io1.cohort_definition_id
	) rechallenge_attempt
	on cases.target_cohort_definition_id = rechallenge_attempt.target_cohort_definition_id
	and cases.outcome_cohort_definition_id = rechallenge_attempt.outcome_cohort_definition_id
	left join
	(
	select dc1.cohort_definition_id as target_cohort_definition_id, io1.cohort_definition_id as outcome_cohort_definition_id, count(distinct dc1.subject_id) as num_cases_rechallenge
	from #target_cohort  dc1
	inner join #outcome_cohort io1
	on dc1.subject_id = io1.subject_id
	and io1.cohort_start_date > dc1.cohort_start_date and io1.cohort_start_date <= dc1.cohort_end_date
	and dc1.cohort_end_date <= dateadd(day,@dechallenge_stop_interval,io1.cohort_start_date)
	left join #outcome_cohort ro0
	on dc1.subject_id = ro0.subject_id
	and io1.cohort_definition_id = ro0.cohort_definition_id
	and ro0.cohort_start_date > dc1.cohort_end_date
	and ro0.cohort_start_date <= dateadd(day, @dechallenge_evaluation_window, dc1.cohort_end_date)   --this should be parameterized to be the dechallenge window required for success/failure
	inner join #target_cohort de1
	on dc1.subject_id = de1.subject_id
	and dc1.cohort_definition_id = de1.cohort_definition_id
	and de1.cohort_start_date > dateadd(day, @dechallenge_evaluation_window, dc1.cohort_end_date)   --using same dechallenge window to detrmine when rechallenge attempt can start
	inner join #outcome_cohort ro1
	on de1.subject_id = ro1.subject_id
	and io1.cohort_definition_id = ro1.cohort_definition_id
	and ro1.cohort_start_date > de1.cohort_start_date
	and ro1.cohort_start_date <= de1.cohort_end_date
	where ro0.subject_id is null --not a dechallenge fail
	group by dc1.cohort_definition_id, io1.cohort_definition_id
	) rechallenge_fail
	on cases.target_cohort_definition_id = rechallenge_fail.target_cohort_definition_id
	and cases.outcome_cohort_definition_id = rechallenge_fail.outcome_cohort_definition_id
) t1
;
