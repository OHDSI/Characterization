context("manual data")

manualData <- file.path(tempdir(), "manual.sqlite")
on.exit(file.remove(manualData), add = TRUE)

manualData2 <- file.path(tempdir(), "manual2.sqlite")
on.exit(file.remove(manualData2), add = TRUE)

test_that("manual data runCharacterizationAnalyses", {
  # this test creates made-up OMOP CDM data
  # and runs runCharacterizationAnalyses on the data
  # to check whether the results are as expected
  connectionDetailsManual <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = manualData
  )
  con <- DatabaseConnector::connect(connectionDetails = connectionDetailsManual)
  on.exit(DatabaseConnector::disconnect(con))
  schema <- "main"

  # add persons  - aggregate covs (age)
  persons <- data.frame(
    person_id = 1:10,
    gender_concept_id = rep(8532, 10),
    year_of_birth = rep(1984, 10)+sample(10,10),
    race_concept_id = rep(1, 10),
    ethnicity_concept_id = rep(1, 10),
    location_id = rep(1, 10),
    provider_id = rep(1, 10),
    care_site_id = rep(1, 10),
    person_source_value = 1:10,
    gender_source_value = rep("female", 10),
    race_source_value = rep("na", 10),
    ethnicity_source_value = rep("na", 10)
  )
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "person",
    data = persons
  )

  # observation period
  obs_period <- data.frame(
    observation_period_id = 1:10,
    person_id = 1:10,
    observation_period_start_date = rep("2000-12-31", 10),
    observation_period_end_date = c("2000-12-31", rep("2020-12-31", 9)),
    period_type_concept_id = rep(1, 10)
  )
  obs_period$observation_period_start_date <- as.Date(obs_period$observation_period_start_date)
  obs_period$observation_period_end_date <- as.Date(obs_period$observation_period_end_date)
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "observation_period",
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
    person_id = c(7, 7, 8, 9, 9, 9, 10),
    condition_concept_id = c(201820, 378253, 201820, 378253, 378253, 378253, 201820),
    condition_era_start_date = c(
      "2011-01-01", "2013-04-03", "2016-01-01",
      "2006-01-04", "2014-08-02", "2014-08-04",
      "2013-01-04"
    ),
    condition_era_end_date = c(
      "2011-01-01", "2013-04-03", "2016-01-01",
      "2006-01-04", "2014-08-02", "2014-08-04",
      "2013-01-04"
    ),
    condition_occurrence_count = rep(1, 7)
  )
  condition_era$condition_era_start_date <- as.Date(condition_era$condition_era_start_date)
  condition_era$condition_era_end_date <- as.Date(condition_era$condition_era_end_date)

  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "condition_era",
    data = condition_era
  )

  # add concept
  concept <- data.frame(
    concept_id = c(201820, 378253),
    concept_name = c("diabetes", "hypertension"),
    domain_id = rep(1, 2),
    vocabulary_id = rep(1, 2),
    concept_class_id = c("Condition", "Condition"),
    standard_concept = rep("S", 2),
    concept_code = rep("Snowmed", 2)
    # ,valid_start_date = NULL,
    # valid_end_date = NULL,
    # invalid_reason = NULL
  )
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "concept",
    data = concept
  )

  # add cohort  - tte/dechal/rechal
  cohort <- data.frame(
    subject_id = c(
      1:10,
      7, 8, 10,
      c(3, 6, 7, 8, 10),
      c(7)
    ),
    cohort_definition_id = c(
      rep(1, 10),
      rep(1, 3),
      rep(2, 5),
      2
    ),
    cohort_start_date = c(
      rep("2018-01-01", 10),
      rep("2018-05-01", 3),
      "2018-01-13", "2018-01-03", rep("2018-01-06", 3),
      "2018-05-24"
    ),
    cohort_end_date = c(
      rep("2018-02-01", 10),
      rep("2018-06-01", 3),
      "2018-02-02", "2018-02-04", rep("2018-02-08", 3),
      "2018-06-05"
    )
  )
  cohort$cohort_start_date <- as.Date(cohort$cohort_start_date)
  cohort$cohort_end_date <- as.Date(cohort$cohort_end_date)
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "cohort",
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
    targetBaselineSettings = Characterization::createTargetBaselineSettings(
      targetIds = 1,
      limitToFirstInNDays = 99999,
      minPriorObservation = 365,
      covariateSettings = FeatureExtraction::createCovariateSettings(
        useDemographicsAge = TRUE,
        useDemographicsGender = TRUE,
        useConditionEraAnyTimePrior = TRUE
      )
    ),

    riskFactorSettings = Characterization::createRiskFactorSettings(
      targetIds = 1,
      outcomeIds = 2,
      limitToFirstInNDays = 99999,
      minPriorObservation = 365,
      outcomeWashoutDays = 30,
      riskWindowStart = 1,
      riskWindowEnd = 90,
      covariateSettings = FeatureExtraction::createCovariateSettings(
        useDemographicsAge = TRUE,
        useDemographicsGender = TRUE,
        useConditionEraAnyTimePrior = TRUE
      )
      ),

    caseSeriesSettings = createCaseSeriesSettings(
      targetIds = 1,
      outcomeIds = 2,
      limitToFirstInNDays = 99999,
      minPriorObservation = 365,
      outcomeWashoutDays = 30,
      riskWindowStart = 1,
      riskWindowEnd = 90,
      caseCovariateSettings = Characterization::createDuringCovariateSettings(
        useConditionEraDuring = TRUE
      )
    )
  )
  runCharacterizationAnalyses(
    connectionDetails = connectionDetailsManual,
    targetDatabaseSchema = schema,
    targetTable = "cohort",
    outcomeDatabaseSchema = schema,
    outcomeTable = "cohort",
    cdmDatabaseSchema = schema,
    characterizationSettings = characterizationSettings,
    outputDirectory = file.path(tempdir(), "result"),
    outputDatabaseSchema = schema,
    outputTable = 'test_char_manual_cohort',
    executionPath = file.path(tempdir(), "execution"),
    csvFilePrefix = "c_",
    databaseId = "1",
    incremental = TRUE,
    threads = 1,
    nTargetJobs = 1,
    minCharacterizationMean = 0,
    minCellCount = 0,
    minSMD = 0,
    minCovariateCount = 0,
    mode = 'PatientLevelPrediction',
    showSubjectId = TRUE
  )

  # check csv results are as expected

  tte <- utils::read.csv(file.path(tempdir(), "result", "c_time_to_event.csv"))

  # check counts - 1-day subsequent missing?
  testthat::expect_true(5 == sum(tte$num_events[tte$outcome_type == "first" & tte$time_scale == "per 1-day"]))
  # subsequent is > 100 days after first drug so not in the 1-day count
  testthat::expect_true(0 == sum(tte$num_events[tte$outcome_type == "subsequent" & tte$time_scale == "per 1-day"]))
  testthat::expect_true(5 == sum(tte$num_events[tte$outcome_type == "first" & tte$time_scale == "per 30-day"]))
  testthat::expect_true(1 == sum(tte$num_events[tte$outcome_type == "subsequent" & tte$time_scale == "per 30-day"]))
  testthat::expect_true(5 == sum(tte$num_events[tte$outcome_type == "first" & tte$time_scale == "per 365-day"]))
  testthat::expect_true(1 == sum(tte$num_events[tte$outcome_type == "subsequent" & tte$time_scale == "per 365-day"]))

  # check times
  testthat::expect_true(sum(c(2, 5, 12) %in% tte$time_to_event[tte$outcome_type == "first" & tte$time_scale == "per 1-day"]) == 3)

  # TODO: check in code whether minCellCount < or <=

  dechal <- utils::read.csv(file.path(tempdir(), "result", "c_dechallenge_rechallenge.csv"))
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

  failed <- utils::read.csv(file.path(tempdir(), "result", "c_rechallenge_fail_case_series.csv"))
  testthat::expect_true(nrow(failed) == 1)
  testthat::expect_true(failed$subject_id == 7)
  testthat::expect_true(failed$dechallenge_exposure_end_date_offset == 31)
  testthat::expect_true(failed$dechallenge_outcome_start_date_offset == 5)
  testthat::expect_true(failed$rechallenge_exposure_start_date_offset == 120)
  testthat::expect_true(failed$rechallenge_outcome_start_date_offset == 143)

  # Target baseline covs
  # =======

  eset <- utils::read.csv(file.path(tempdir(), "result", "c_execution_settings.csv"))
  testthat::expect_true(nrow(eset) == 1)


  # targetId = 1, limitToFirstInNDays = 99999,minPriorObservation = 365,
  tset <- utils::read.csv(file.path(tempdir(), "result", "c_target_settings.csv"))
  testthat::expect_true(tset$target_id == 1)
  testthat::expect_true(tset$limit_to_first_in_n_days == 99999)
  testthat::expect_true(tset$min_prior_observation == 365)
  testthat::expect_true(tset$characterization_target_id == tset$target_id*10)

  attrition <- utils::read.csv(file.path(tempdir(), "result", "c_attrition.csv"))
  # there should be 9 people as the first subject has cohort date outside observation
  testthat::expect_true(attrition$N[attrition$cohort_definition_id==10] == 9)

  # useDemographicsAge = TRUE, useDemographicsGender = TRUE, useConditionEraAnyTimePrior = TRUE
  covs <- utils::read.csv(file.path(tempdir(), "result", "c_target_covariates.csv"))
  # gender 8532001
  testthat::expect_true(8532001 %in% covs$covariate_id)

  maxCount <- length(unique(cohort$subject_id[cohort$cohort_definition_id == 1]))
  testthat::expect_true(sum(covs$sum_value <= maxCount) == nrow(covs))

  # data is all female so make sure female cov has average_value of 1
  testthat::expect_true(covs$average_value[covs$covariate_id == 8532001] == 1)
  testthat::expect_true(covs$sum_value[covs$covariate_id == 8532001] == attrition$N[attrition$cohort_definition_id==10])

  covs_cont <- utils::read.csv(file.path(tempdir(), "result", "c_target_covariates_continuous.csv"))
  testthat::expect_true(1002 %in% covs_cont$covariate_id)
  testthat::expect_true(covs_cont$count_value == 9)


  # risk factor - check SMD
  rf_covs <- utils::read.csv(file.path(tempdir(), "result", "c_risk_factor_covariates.csv"))
  # 378253 - 7 : 2013-04-03, 9 : 2006-01-04/2014-08-02/2014-08-04
  # 7 had outcome 5 days after index and ~5 months after index
  # 9 does not have outcome

  testthat::expect_true(rf_covs$non_case_sum_value[rf_covs$covariate_id == 378253201] == 1)
  testthat::expect_true(rf_covs$case_sum_value[rf_covs$covariate_id == 378253201] == 1)
  # 5 cases and 4 non-cases
  testthat::expect_true(rf_covs$non_case_average_value[rf_covs$covariate_id == 378253201] == 1/4)
  testthat::expect_true(rf_covs$case_average_value[rf_covs$covariate_id == 378253201] == 1/5)

  # now make sure SMD is correct
  # 4 non-cases and 1 has outcome, 5-cases and 1 has outcome
  nonCases <- 4
  nonCaseMean <- rf_covs$non_case_average_value[rf_covs$covariate_id == 378253201]
  nonCaseOnes <- 1
  nonCaseZeros <- 3
  cases <- 5
  caseMean <- rf_covs$case_average_value[rf_covs$covariate_id == 378253201]
  caseOnes <- 1
  caseZeros <- 4

  meanDiff <- caseMean - nonCaseMean
  nonCaseSD <- sqrt((nonCaseOnes*(1-nonCaseMean)^2 + nonCaseZeros*(0-nonCaseMean)^2)/(nonCases-1))
  caseSD <- sqrt((caseOnes*(1-caseMean)^2 + caseZeros*(0-caseMean)^2)/(cases-1))
  pooledSD <- sqrt((caseSD^2+nonCaseSD^2)/2)
  manualSMD <- meanDiff/pooledSD

  testthat::expect_equal(
    rf_covs$standardized_mean_difference[rf_covs$covariate_id == 378253201],
    manualSMD, tolerance = 0.01
    )

  # test when a non-case or case is 0

  nonCases <- 4
  nonCaseMean <- rf_covs$non_case_average_value[rf_covs$covariate_id == 201820201]
  nonCaseOnes <- 0
  nonCaseZeros <- 4
  cases <- 5
  caseMean <- rf_covs$case_average_value[rf_covs$covariate_id == 201820201]
  caseOnes <- 3
  caseZeros <- 2

  meanDiff <- caseMean - nonCaseMean
  nonCaseSD <- sqrt((nonCaseOnes*(1-nonCaseMean)^2 + nonCaseZeros*(0-nonCaseMean)^2)/(nonCases-1))
  caseSD <- sqrt((caseOnes*(1-caseMean)^2 + caseZeros*(0-caseMean)^2)/(cases-1))
  pooledSD <- sqrt((caseSD^2+nonCaseSD^2)/2)
  manualSMD <- meanDiff/pooledSD

  testthat::expect_equal(
    rf_covs$standardized_mean_difference[rf_covs$covariate_id == 201820201],
    manualSMD, tolerance = 0.01
  )


  rf_cont <- utils::read.csv(file.path(tempdir(), "result", "c_risk_factor_covariates_continuous.csv"))

  nonCases <- 4
  nonCaseMean <- rf_cont$non_case_average_value[rf_cont$covariate_id == 1002]
  nonCaseSd <- rf_cont$non_case_standard_deviation[rf_cont$covariate_id == 1002]
  cases <- 5
  caseMean <- rf_cont$case_average_value[rf_cont$covariate_id == 1002]
  caseSd <- rf_cont$case_standard_deviation[rf_cont$covariate_id == 1002]

  meanDiff <- caseMean - nonCaseMean
  pooledSD <- sqrt((caseSd^2+nonCaseSd^2)/2)
  manualSMD <- meanDiff/pooledSD

  testthat::expect_equal(
    rf_cont$standardized_mean_difference[rf_cont$covariate_id == 1002],
    manualSMD, tolerance = 0.01
  )


})



test_that("manual data checking ... works", {
  # this test creates made-up OMOP CDM data
  # and runs runCharacterizationAnalyses on the data
  # to check whether the results are as expected
  connectionDetailsManual <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = manualData2
  )
  con <- DatabaseConnector::connect(connectionDetails = connectionDetailsManual)
  on.exit(DatabaseConnector::disconnect(con))
  schema <- "main"

  # add persons  - aggregate covs (age)
  persons <- data.frame(
    person_id = 1:10,
    gender_concept_id = rep(8532, 10),
    year_of_birth = rep(2000, 10),
    race_concept_id = rep(1, 10),
    ethnicity_concept_id = rep(1, 10),
    location_id = rep(1, 10),
    provider_id = rep(1, 10),
    care_site_id = rep(1, 10),
    person_source_value = 1:10,
    gender_source_value = rep("female", 10),
    race_source_value = rep("na", 10),
    ethnicity_source_value = rep("na", 10)
  )
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "person",
    data = persons
  )

  # observation period
  obs_period <- data.frame(
    observation_period_id = 1:10,
    person_id = 1:10,
    observation_period_start_date = rep("2000-12-31", 10),
    observation_period_end_date = c("2000-12-31", rep("2020-12-31", 9)),
    period_type_concept_id = rep(1, 10)
  )
  obs_period$observation_period_start_date <- as.Date(obs_period$observation_period_start_date)
  obs_period$observation_period_end_date <- as.Date(obs_period$observation_period_end_date)
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "observation_period",
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
    person_id = c(7, 7, 8, 9, 9, 9, 10),
    condition_concept_id = c(201820, 378253, 201820, 378253, 378253, 378253, 201820),
    condition_era_start_date = c(
      "2011-01-01", "2013-04-03", "2016-01-01",
      "2006-01-04", "2014-08-02", "2014-08-04",
      "2013-01-04"
    ),
    condition_era_end_date = c(
      "2011-01-01", "2013-04-03", "2016-01-01",
      "2006-01-04", "2014-08-02", "2014-08-04",
      "2013-01-04"
    ),
    condition_occurrence_count = rep(1, 7)
  )
  condition_era$condition_era_start_date <- as.Date(condition_era$condition_era_start_date)
  condition_era$condition_era_end_date <- as.Date(condition_era$condition_era_end_date)

  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "condition_era",
    data = condition_era
  )

  # add concept
  concept <- data.frame(
    concept_id = c(201820, 378253),
    concept_name = c("diabetes", "hypertension"),
    domain_id = rep(1, 2),
    vocabulary_id = rep(1, 2),
    concept_class_id = c("Condition", "Condition"),
    standard_concept = rep("S", 2),
    concept_code = rep("Snowmed", 2)
    # ,valid_start_date = NULL,
    # valid_end_date = NULL,
    # invalid_reason = NULL
  )
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "concept",
    data = concept
  )

  # add cohort  - tte/dechal/rechal
  # person 6 has the outcome just before the exposure
  cohort <- data.frame(
    subject_id = c(
      1:10,
      7, 8, 10,
      c(3, 6, 7, 8, 10),
      c(7),
      6
    ),
    cohort_definition_id = c(
      rep(1, 10),
      rep(1, 3),
      rep(2, 5),
      2,
      2
    ),
    cohort_start_date = c(
      rep("2018-01-01", 10),
      rep("2018-05-01", 3),
      "2018-01-13", "2018-01-03", rep("2018-01-06", 3),
      "2018-05-24",
      "2017-12-29"
    ),
    cohort_end_date = c(
      rep("2018-02-01", 10),
      rep("2018-06-01", 3),
      "2018-02-02", "2018-02-04", rep("2018-02-08", 3),
      "2018-06-05",
      "2017-12-29"
    )
  )
  cohort$cohort_start_date <- as.Date(cohort$cohort_start_date)
  cohort$cohort_end_date <- as.Date(cohort$cohort_end_date)
  DatabaseConnector::insertTable(
    connection = con,
    databaseSchema = schema,
    tableName = "cohort",
    data = cohort
  )

  # add checks here

})
