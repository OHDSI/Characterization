context("manual data")

manualData <- file.path(tempdir(), 'manual.sqlite')
on.exit(file.remove(manualData), add = TRUE)

test_that("manual data runCharacterizationAnalyses", {

  # this test creates made-up OMOP CDM data
  # and runs runCharacterizationAnalyses on the data
  # to check whether the results are as expected
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = 'sqlite',
  server = manualData
  )
con <- DatabaseConnector::connect(connectionDetails = connectionDetails)
schema <- 'main'

# add persons  - aggregate covs (age)
persons <- data.frame(
  person_id = 1:10,
  gender_concept_id = rep(8532, 10),
  year_of_birth = rep(2000, 10),
  race_concept_id = rep(1, 10),
  ethnicity_concept_id = rep(1, 10),
  location_id = rep(1,10),
  provider_id = rep(1,10),
  care_site_id = rep(1,10),
  person_source_value = 1:10,
  gender_source_value = rep('female', 10),
  race_source_value = rep('na', 10),
  ethnicity_source_value = rep('na', 10)
)
DatabaseConnector::insertTable(
  connection = con,
  databaseSchema = schema,
  tableName = 'person',
  data = persons
    )

# observation period
obs_period <- data.frame(
  observation_period_id = 1:10,
  person_id = 1:10,
  observation_period_start_date = rep('2000-12-31', 10),
  observation_period_end_date = c('2000-12-31', rep('2020-12-31', 9)),
  period_type_concept_id = rep(1,10)
)
obs_period$observation_period_start_date <- as.Date(obs_period$observation_period_start_date)
obs_period$observation_period_end_date <- as.Date(obs_period$observation_period_end_date)
DatabaseConnector::insertTable(
  connection = con,
  databaseSchema = schema,
  tableName = 'observation_period',
  data = obs_period
)
# person 1 has 1 day obs
# person 2-6 has no events
# person 7 has diabetes at 10, headache at 12
# person 8 has diabetes at 13
# person 9 has headache multiple times
# person 10 has diabetes at 14
# add conditions - aggregate covs (conditions)

condition_era <- data.frame(
  condition_era_id = 1:7,
  person_id = c(7,7,8, 9,9,9,10),
  condition_concept_id = c(201820, 378253,201820,378253,378253,378253, 201820),
  condition_era_start_date = c('2011-01-01', '2013-04-03', '2016-01-01',
                               '2006-01-04', '2014-08-02', '2014-08-04',
                               '2013-01-04'),
  condition_era_end_date = c('2011-01-01', '2013-04-03', '2016-01-01',
                             '2006-01-04', '2014-08-02', '2014-08-04',
                             '2013-01-04'),
  condition_occurrence_count = rep(1, 7)
)
condition_era$condition_era_start_date <- as.Date(condition_era$condition_era_start_date)
condition_era$condition_era_end_date <- as.Date(condition_era$condition_era_end_date)

DatabaseConnector::insertTable(
  connection = con,
  databaseSchema = schema,
  tableName = 'condition_era',
  data = condition_era
)

# add concept
concept <- data.frame(
  concept_id = c(201820,378253),
  concept_name = c('diabetes', 'hypertension'),
  domain_id = rep(1,2),
  vocabulary_id = rep(1,2),
  concept_class_id = c('Condition', 'Condition'),
  standard_concept = rep('S',2),
  concept_code = rep('Snowmed',2)
  #,valid_start_date = NULL,
  #valid_end_date = NULL,
  #invalid_reason = NULL
)
DatabaseConnector::insertTable(
  connection = con,
  databaseSchema = schema,
  tableName = 'concept',
  data = concept
)

# add cohort  - tte/dechal/rechal
cohort <- data.frame(
  subject_id = c(
    1:10,
    7,8,10,
    c(3,6,7,8,10),
    c(7)
  ),
  cohort_definition_id = c(
    rep(1,10),
    rep(1,3),
    rep(2, 5),
    2
  ),
  cohort_start_date = c(
    rep('2018-01-01', 10),
    rep('2018-05-01',3),
    '2018-01-13','2018-01-03',rep('2018-01-06',3),
    '2018-05-24'
  ),
  cohort_end_date = c(
    rep('2018-02-01', 10),
    rep('2018-06-01',3),
    '2018-02-02','2018-02-04',rep('2018-02-08',3),
    '2018-06-05'
  )
)
cohort$cohort_start_date <- as.Date(cohort$cohort_start_date)
cohort$cohort_end_date <- as.Date(cohort$cohort_end_date)
DatabaseConnector::insertTable(
  connection = con,
  databaseSchema = schema,
  tableName = 'cohort',
  data = cohort
)

# create settings and run
characterizationSettings <- Characterization::createCharacterizationSettings(
  timeToEventSettings = Characterization::createTimeToEventSettings(
    targetIds = 1,
    outcomeIds = 2
    ),
  dechallengeRechallengeSettings = Characterization::createDechallengeRechallengeSettings(
    targetIds = 1,
    outcomeIds = 2
    ),
  aggregateCovariateSettings = Characterization::createAggregateCovariateSettings(
    targetIds = 1,
    outcomeIds = 2,
    minPriorObservation = 365,
    outcomeWashoutDays = 30,
    riskWindowStart = 1,
    riskWindowEnd = 90,
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsAge = T,
      useDemographicsGender = T,
      useConditionEraAnyTimePrior = T
      ),
    caseCovariateSettings = Characterization::createDuringCovariateSettings(useConditionEraDuring = T),
    casePreTargetDuration = 365*5
    )
)
Characterization::runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  targetDatabaseSchema = schema,
  targetTable = 'cohort',
  outcomeDatabaseSchema = schema,
  outcomeTable = 'cohort',
  cdmDatabaseSchema = schema,
  characterizationSettings = characterizationSettings,
  outputDirectory = file.path(tempdir(), 'result'),
  executionPath = file.path(tempdir(), 'execution'),
  csvFilePrefix = 'c_',
  databaseId = '1',
  incremental = T,
  threads = 1,
  minCharacterizationMean = 0.0001,
  minCellCount = NULL,
  showSubjectId = T
  )

# check csv results are as expected

tte <- read.csv(file.path(tempdir(), 'result','c_time_to_event.csv'))

# check counts - 1-day subsequent missing?
testthat::expect_true(5 == sum(tte$num_events[tte$outcome_type == 'first' & tte$time_scale == 'per 1-day']))
#subsequent is > 100 days after first drug so not in the 1-day count
testthat::expect_true(0 == sum(tte$num_events[tte$outcome_type == 'subsequent' & tte$time_scale == 'per 1-day']))
testthat::expect_true(5 == sum(tte$num_events[tte$outcome_type == 'first' & tte$time_scale == 'per 30-day']))
testthat::expect_true(1 == sum(tte$num_events[tte$outcome_type == 'subsequent' & tte$time_scale == 'per 30-day']))
testthat::expect_true(5 == sum(tte$num_events[tte$outcome_type == 'first' & tte$time_scale == 'per 365-day']))
testthat::expect_true(1 == sum(tte$num_events[tte$outcome_type == 'subsequent' & tte$time_scale == 'per 365-day']))

# check times
testthat::expect_true(sum(c(2,5,12) %in% tte$time_to_event[tte$outcome_type == 'first' & tte$time_scale == 'per 1-day']) == 3)

# TODO: check in code whether minCellCount < or <=

dechal <- read.csv(file.path(tempdir(), 'result','c_dechallenge_rechallenge.csv'))
testthat::expect_true(dechal$num_exposure_eras == 13)
testthat::expect_true(dechal$num_persons_exposed == 10)
testthat::expect_true(dechal$num_cases == 6)
testthat::expect_true(dechal$dechallenge_attempt == 5)
testthat::expect_true(dechal$dechallenge_success == 5)
testthat::expect_true(dechal$rechallenge_attempt == 3)

# one person has a rechal and event stops when second drug exposure stops
testthat::expect_true(dechal$rechallenge_fail == 1)
testthat::expect_true(dechal$rechallenge_success == 2)
testthat::expect_true(dechal$pct_rechallenge_fail == 0.3333333)

failed <- read.csv(file.path(tempdir(), 'result','c_rechallenge_fail_case_series.csv'))
testthat::expect_true(nrow(failed) == 1)
testthat::expect_true(failed$subject_id == 7)
testthat::expect_true(failed$dechallenge_exposure_end_date_offset == 31)
testthat::expect_true(failed$dechallenge_outcome_start_date_offset == 5)
testthat::expect_true(failed$rechallenge_exposure_start_date_offset == 120)
testthat::expect_true(failed$rechallenge_outcome_start_date_offset == 143)

# Aggregate covs
#=======
counts <- read.csv(file.path(tempdir(), 'result','c_cohort_counts.csv'))
# when restricted to first exposure 5 people have outcome
testthat::expect_true(counts$row_count[counts$cohort_type == 'Cases'] == 5)
# target is 9 because 1 has insufficient min prior obs
testthat::expect_true(counts$row_count[counts$cohort_type == 'Target' &
                                         counts$target_cohort_id == 1] == 9)
# make sure outcome is there a has count of 5
testthat::expect_true(counts$row_count[counts$cohort_type == 'Target' &
                                         counts$target_cohort_id == 2] == 5)

# Tall should not have first restriction
testthat::expect_true(counts$row_count[counts$cohort_type == 'Tall' &
                                         counts$target_cohort_id == 1] == 13)
testthat::expect_true(counts$person_count[counts$cohort_type == 'Tall' &
                                         counts$target_cohort_id == 1] == 10)
# make sure outcome is there a has count of 6 and 5
testthat::expect_true(counts$row_count[counts$cohort_type == 'Tall' &
                                         counts$target_cohort_id == 2] == 6)
testthat::expect_true(counts$person_count[counts$cohort_type == 'Tall' &
                                         counts$target_cohort_id == 2] == 5)

covs <- read.csv(file.path(tempdir(), 'result','c_covariates.csv'))

# checks all females
testthat::expect_true(covs$average_value[covs$covariate_id == 8532001 & covs$cohort_type == 'Cases'] == 1)
testthat::expect_true(covs$average_value[covs$covariate_id == 8532001 & covs$cohort_type == 'Target' &
                                           covs$target_cohort_id == 1] == 1)
testthat::expect_true(covs$average_value[covs$covariate_id == 8532001 & covs$cohort_type == 'Target' &
                                           covs$target_cohort_id == 2] == 1)

## TODO: check diabetes and hypertensions
#covs$covariate_id
# 201820 7,8 and 10 have in history and all are cases
ind <- covs$covariate_id == 201820201 & covs$target_cohort_id == 1 &
  covs$cohort_type == 'Cases'
testthat::expect_true(covs$sum_value[ind] == 3)
testthat::expect_true(covs$average_value[ind] == 3/5)

ind <- covs$covariate_id == 201820201 & covs$target_cohort_id == 1 &
  covs$cohort_type == 'Target'
testthat::expect_true(covs$sum_value[ind] == 3)
testthat::expect_equal(covs$average_value[ind],  3/9, tolerance = 0.01)

# 378253 7,9 (9 multiple times) but 9 not a case
ind <- covs$covariate_id == 378253201 & covs$target_cohort_id == 1 &
  covs$cohort_type == 'Cases'
testthat::expect_true(covs$sum_value[ind] == 1)
testthat::expect_true(covs$average_value[ind] == 1/5)

ind <- covs$covariate_id == 378253201 & covs$target_cohort_id == 1 &
  covs$cohort_type == 'Target'
testthat::expect_true(covs$sum_value[ind] == 2)
testthat::expect_equal(covs$average_value[ind],  2/9, tolerance = 0.01)


covs_cont <- read.csv(file.path(tempdir(), 'result','c_covariates_continuous.csv'))

# checks age in years
testthat::expect_true(covs_cont$average_value[covs_cont$covariate_id == 1002 & covs_cont$cohort_type == 'Cases'] == 18)
testthat::expect_true(covs_cont$count_value[covs_cont$covariate_id == 1002 & covs_cont$cohort_type == 'Cases'] == 5)
testthat::expect_true(covs_cont$average_value[covs_cont$covariate_id == 1002 & covs_cont$cohort_type == 'Target' &
                                           covs_cont$target_cohort_id == 1] == 18)
testthat::expect_true(covs_cont$count_value[covs_cont$covariate_id == 1002 & covs_cont$cohort_type == 'Target' &
                                                covs_cont$target_cohort_id == 1] == 9)
testthat::expect_true(covs_cont$average_value[covs_cont$covariate_id == 1002 & covs_cont$cohort_type == 'Target' &
                                                covs_cont$target_cohort_id == 2] == 18)
testthat::expect_true(covs_cont$count_value[covs_cont$covariate_id == 1002 & covs_cont$cohort_type == 'Target' &
                                              covs_cont$target_cohort_id == 2] == 5)


})
