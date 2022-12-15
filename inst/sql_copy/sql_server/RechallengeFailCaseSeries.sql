select * into #target_cohort
from @target_database_schema.@target_table
where cohort_definition_id in (@target_ids)
;

select * into #outcome_cohort
from @outcome_database_schema.@outcome_table
where cohort_definition_id in (@outcome_ids)
;

--extract persons that are rechallenge fails
select

  '@database_id' as database_id,
  @dechallenge_stop_interval as dechallenge_stop_interval,
  @dechallenge_evaluation_window as dechallenge_evaluation_window,
  dc1.cohort_definition_id as target_cohort_definition_id,
  io1.cohort_definition_id as outcome_cohort_definition_id,
  dense_rank() over (partition by dc1.cohort_definition_id, io1.cohort_definition_id order by datediff(day, dc0.cohort_start_date, dc1.cohort_start_date), dc1.subject_id) as person_key,
  {@show_subject_id}?{dc1.subject_id}:{NULL as subject_id},  --this is the field that we would want to allow parameter to make nullable or not export
  dc1.era_number as dechallenge_exposure_number,
  datediff(day, dc0.cohort_start_date, dc1.cohort_start_date) as dechallenge_exposure_start_date_offset,
  datediff(day, dc0.cohort_start_date, dc1.cohort_end_date) as dechallenge_exposure_end_date_offset,
  io1.era_number as dechallenge_outcome_number,
  datediff(day, dc0.cohort_start_date, io1.cohort_start_date) as dechallenge_outcome_start_date_offset,
  de1.era_number as rechallenge_exposure_number,
  datediff(day, dc0.cohort_start_date, de1.cohort_start_date) as rechallenge_exposure_start_date_offset,
  datediff(day, dc0.cohort_start_date, de1.cohort_end_date) as rechallenge_exposure_end_date_offset,
  ro1.era_number as rechallenge_outcome_number,
  datediff(day, dc0.cohort_start_date, ro1.cohort_start_date) as rechallenge_outcome_start_date_offset

into #fail_case_series

from (select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date) as era_number from #target_cohort)  dc0
  inner join
  (select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date) as era_number from #target_cohort)  dc1
  on dc0.subject_id = dc1.subject_id
  and dc0.cohort_definition_id = dc1.cohort_definition_id
  and dc0.era_number = 1
	inner join (select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date) as era_number from #outcome_cohort) io1
	on dc1.subject_id = io1.subject_id
	and io1.cohort_start_date > dc1.cohort_start_date and io1.cohort_start_date <= dc1.cohort_end_date
	and dc1.cohort_end_date <= dateadd(day,@dechallenge_stop_interval,io1.cohort_start_date) -- exposure ends shortly after outcome starts
	left join #outcome_cohort ro0 -- used to exclude people who have the outcome between exposure or next eligible time
	on dc1.subject_id = ro0.subject_id
	and io1.cohort_definition_id = ro0.cohort_definition_id
	and ro0.cohort_start_date > dc1.cohort_end_date
	and ro0.cohort_start_date <= dateadd(day,@dechallenge_evaluation_window,dc1.cohort_end_date)   --this should be parameterized to be the dechallenge window required for success/failure
	inner join (select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date) as era_number from #target_cohort) de1
	on dc1.subject_id = de1.subject_id
	and dc1.cohort_definition_id = de1.cohort_definition_id
	and de1.cohort_start_date > dateadd(day,@dechallenge_evaluation_window,dc1.cohort_end_date)   --using same dechallenge window to detrmine when rechallenge attempt can start
	inner join (select *, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date) as era_number from #outcome_cohort) ro1
	on de1.subject_id = ro1.subject_id
	and io1.cohort_definition_id = ro1.cohort_definition_id
	and ro1.cohort_start_date > de1.cohort_start_date
	and ro1.cohort_start_date <= de1.cohort_end_date
	where ro0.subject_id is null
;
