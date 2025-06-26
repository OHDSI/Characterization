# library(Characterization)
# library(testthat)

context("runCharacterizationAnalyses")

test_that("runCharacterizationAnalyses", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)

  timeToEventSettings1 <- createTimeToEventSettings(
    targetIds = 1,
    outcomeIds = c(3, 4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    targetIds = 2,
    outcomeIds = c(3, 4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  aggregateCovariateSettings1 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsRace = T
    )
  )

  aggregateCovariateSettings2 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsRace = T
    )
  )

  characterizationSettings <- createCharacterizationSettings(
    timeToEventSettings = list(
      timeToEventSettings1,
      timeToEventSettings2
    ),
    dechallengeRechallengeSettings = list(
      dechallengeRechallengeSettings
    ),
    aggregateCovariateSettings = list(
      aggregateCovariateSettings1,
      aggregateCovariateSettings2
    )
  )

  testthat::expect_true(
    class(characterizationSettings) == "characterizationSettings"
  )

  testthat::expect_true(
    length(characterizationSettings$timeToEventSettings) == 2
  )
  testthat::expect_true(
    length(characterizationSettings$dechallengeRechallengeSettings) == 1
  )
  testthat::expect_true(
    length(characterizationSettings$aggregateCovariateSettings) == 2
  )

  tempFile <- tempfile(fileext = ".json")
  on.exit(unlink(tempFile))
  saveLoc <- saveCharacterizationSettings(
    settings = characterizationSettings,
    fileName = tempFile
  )

  testthat::expect_true(file.exists(tempFile))

  loadedSettings <- loadCharacterizationSettings(
    fileName = tempFile
  )

  # In R, empty arrays are automatically of type 'logical.' When loading JSON
  # they are currently automatically of type 'list'. Neither is right or wrong,
  # so ignoring distinction:
  convertEmptyListToEmptyLogical <- function(object) {
    if (is.list(object)) {
      if (length(object) == 0) {
        return(vector(mode = "logical", length = 0))
      } else {
        return(lapply(object, convertEmptyListToEmptyLogical))
      }
    } else {
      return(object)
    }
  }
  testthat::expect_equivalent(characterizationSettings, convertEmptyListToEmptyLogical(loadedSettings))

  tempFolder <- tempfile("Characterization")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

  runCharacterizationAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationSettings = characterizationSettings,
    outputDirectory = file.path(tempFolder, "result"),
    executionPath = file.path(tempFolder, "execution"),
    csvFilePrefix = "c_",
    databaseId = "1",
    incremental = T,
    minCharacterizationMean = 0.01,
    threads = 1
  )

  testthat::expect_true(
    dir.exists(file.path(tempFolder, "result"))
  )

  # check csv files
  testthat::expect_true(
    length(dir(file.path(tempFolder, "result"))) > 0
  )

  # check cohort details is saved
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_cohort_details.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_settings.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_analysis_ref.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_covariate_ref.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_covariates.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_covariates_continuous.csv"))
  )

  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_dechallenge_rechallenge.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_rechallenge_fail_case_series.csv"))
   )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_time_to_event.csv"))
  )

  # make sure both tte runs are in the csv
  tte <- readr::read_csv(
    file = file.path(tempFolder, "result", "c_time_to_event.csv"),
    show_col_types = FALSE
  )
  testthat::expect_equivalent(
    unique(tte$target_cohort_definition_id),
    c(1, 2)
  )
})



manualDataMin <- file.path(tempdir(), "manual_min.sqlite")
on.exit(file.remove(manualDataMin), add = TRUE)

test_that("min cell count works", {
  tempFolder <- tempfile("CharacterizationMin")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = manualDataMin
  )
  con <- DatabaseConnector::connect(connectionDetails = connectionDetails)
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
      casePreTargetDuration = 365 * 5
    )
  )

  Characterization::runCharacterizationAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationSettings = characterizationSettings,
    outputDirectory = file.path(tempFolder, "result_mincell"),
    executionPath = file.path(tempFolder, "execution_mincell"),
    csvFilePrefix = "c_",
    databaseId = "1",
    incremental = F,
    minCharacterizationMean = 0.001,
    threads = 1,
    minCellCount = 1000000
  )

  testthat::expect_true(
    file.exists(file.path(tempFolder, "result_mincell", "c_time_to_event.csv"))
  )
  res <- readr::read_csv(file.path(tempFolder, "result_mincell", "c_time_to_event.csv"))
  # all values will be censored to -1 times the minCellCount of 10000000
  testthat::expect_true(sum(res$num_events == -1000000) == length(res$num_events))

  testthat::expect_true(
    file.exists(file.path(tempFolder, "result_mincell", "c_dechallenge_rechallenge.csv"))
  )
  res <- readr::read_csv(file.path(tempFolder, "result_mincell", "c_dechallenge_rechallenge.csv"))
  # all values will be censored to -1 times the minCellCount of 10000000
  testthat::expect_true(sum(res$num_exposure_eras == -1000000) == length(res$num_exposure_eras))
  testthat::expect_true(sum(res$num_cases == -1000000) == length(res$num_cases))
  testthat::expect_true(sum(res$dechallenge_attempt == -1000000) == length(res$dechallenge_attempt))
  testthat::expect_true(sum(res$dechallenge_success == -1000000) == length(res$dechallenge_success))
  testthat::expect_true(sum(res$dechallenge_fail == -1000000) == length(res$dechallenge_fail))

  testthat::expect_true(sum(res$rechallenge_attempt == -1000000) == length(res$rechallenge_attempt))
  testthat::expect_true(sum(res$rechallenge_success == -1000000) == length(res$rechallenge_success))
  testthat::expect_true(sum(res$rechallenge_fail == -1000000) == length(res$rechallenge_fail))

  testthat::expect_true(sum(is.na(res$pct_dechallenge_attempt)) == length(res$pct_dechallenge_attempt))
  testthat::expect_true(sum(is.na(res$pct_dechallenge_success)) == length(res$pct_dechallenge_success))
  testthat::expect_true(sum(is.na(res$pct_dechallenge_fail)) == length(res$pct_dechallenge_fail))

  testthat::expect_true(sum(is.na(res$pct_rechallenge_attempt)) == length(res$pct_rechallenge_attempt))
  testthat::expect_true(sum(is.na(res$pct_rechallenge_success)) == length(res$pct_rechallenge_success))
  testthat::expect_true(sum(is.na(res$pct_rechallenge_fail)) == length(res$pct_rechallenge_fail))


  testthat::expect_true(
    file.exists(file.path(tempFolder, "result_mincell", "c_cohort_counts.csv"))
  )
  res <- readr::read_csv(file.path(tempFolder, "result_mincell", "c_cohort_counts.csv"))
  testthat::expect_true(sum(res$row_count == -1000000) == length(res$row_count))
  testthat::expect_true(sum(res$person_count == -1000000) == length(res$person_count))


  testthat::expect_true(
    file.exists(file.path(tempFolder, "result_mincell", "c_covariates.csv"))
  )
  res <- readr::read_csv(file.path(tempFolder, "result_mincell", "c_covariates.csv"))
  testthat::expect_true(sum(res$sum_value == -1000000) == length(res$sum_value))
  testthat::expect_true(sum(is.na(res$average_value)) == length(res$average_value))

  testthat::expect_true(
    file.exists(file.path(tempFolder, "result_mincell", "c_covariates_continuous.csv"))
  )
  res <- readr::read_csv(file.path(tempFolder, "result_mincell", "c_covariates_continuous.csv"))
  testthat::expect_true(sum(res$count_value == -1000000) == length(res$count_value))
  testthat::expect_true(sum(is.na(res$average_value)) == length(res$average_value))
  testthat::expect_true(sum(is.na(res$p_10_value)) == length(res$p_10_value))
  testthat::expect_true(sum(is.na(res$p_90_value)) == length(res$p_90_value))
  testthat::expect_true(sum(is.na(res$p_25_value)) == length(res$p_25_value))
  testthat::expect_true(sum(is.na(res$p_75_value)) == length(res$p_75_value))
  testthat::expect_true(sum(is.na(res$min_value)) == length(res$min_value))
  testthat::expect_true(sum(is.na(res$max_value)) == length(res$max_value))
})



test_that("checking the batch csv aggregation", {

  tempFolder <- tempfile("Characterization")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

  executionPath <- testthat::test_path("testdata", "execution")

  if(!dir.exists(file.path(tempFolder,'aggCvs'))){
    dir.create(file.path(tempFolder,'aggCvs'), recursive = T)
  }
  if(!dir.exists(file.path(tempFolder,'aggCvs2'))){
    dir.create(file.path(tempFolder,'aggCvs2'), recursive = T)
  }
  if(!dir.exists(file.path(tempFolder,'aggCvs3'))){
    dir.create(file.path(tempFolder,'aggCvs3'), recursive = T)
  }

# checking the batch csv aggregation
Characterization:::aggregateCsvs(
  executionPath = executionPath,
  outputFolder = file.path(tempFolder,'aggCvs'),
  csvFilePrefix = ''
)

Characterization:::aggregateCsvsBatch(
  executionPath = executionPath,
  outputFolder = file.path(tempFolder,'aggCvs2'),
  csvFilePrefix = ''
)

Characterization:::aggregateCsvsBatch(
  executionPath = executionPath,
  outputFolder = file.path(tempFolder,'aggCvs3'),
  csvFilePrefix = '',
  batchSize = 1
)

#check files are the same using default batch
files <- dir(file.path(tempFolder,'aggCvs'), pattern = 'csv')
for(i in 1:length(files)){
  d1 <- readr::read_csv(file.path(tempFolder,'aggCvs', files[i]),
                        show_col_types = F)
  d2 <- readr::read_csv(file.path(tempFolder,'aggCvs2', files[i]),
                        show_col_types = F)
  testthat::expect_true(all.equal(d1,d2))
}

# when batchsize is 1
files <- dir(file.path(tempFolder,'aggCvs'), pattern = 'csv')
for(i in 1:length(files)){
  d1 <- readr::read_csv(file.path(tempFolder,'aggCvs', files[i]),
                        show_col_types = F)
  d2 <- readr::read_csv(file.path(tempFolder,'aggCvs3', files[i]),
                        show_col_types = F)
  testthat::expect_true(all.equal(d1,d2))
}

# make sure it still works if re-executed (no left over files)
Characterization:::aggregateCsvsBatch(
  executionPath = executionPath,
  outputFolder = file.path(tempFolder,'aggCvs3'),
  csvFilePrefix = '',
  batchSize = 1
)
files <- dir(file.path(tempFolder,'aggCvs'), pattern = 'csv')
for(i in 1:length(files)){
  d1 <- readr::read_csv(file.path(tempFolder,'aggCvs', files[i]),
                        show_col_types = F)
  d2 <- readr::read_csv(file.path(tempFolder,'aggCvs3', files[i]),
                        show_col_types = F)
  testthat::expect_true(all.equal(d1,d2))
}

})
