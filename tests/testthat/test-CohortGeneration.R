context("CohortGeneration")

sqlitePath <- tempfile(fileext = ".sqlite")
withr::defer(unlink(sqlitePath, force = TRUE), testthat::teardown_env())

test_that("generateOutcomeEras executes on sqlite", {
  skipIfCreateTargetCohortSqlUnavailable()

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = sqlitePath
  )
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = "main",
    tableName = "cohort",
    data = data.frame(
      cohort_definition_id = c(3, 3, 3, 3),
      subject_id = c(1, 1, 1, 2),
      cohort_start_date = as.Date(c("2020-01-01", "2020-06-01", "2021-07-01", "2020-03-01")),
      cohort_end_date = as.Date(c("2020-01-10", "2020-06-10", "2021-07-10", "2020-03-05"))
    )
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = "CREATE TABLE main.outcome_era (cohort_definition_id BIGINT, outcome_washout BIGINT, subject_id BIGINT, cohort_start_date DATE, cohort_end_date DATE);",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "OutcomeEras.sql",
    packageName = "Characterization",
    dbms = "sqlite",
    tempEmulationSchema = "main",
    characterization_schema = "main",
    outcome_era_table = "outcome_era",
    outcome_ids = "3",
    outcome_washout = 365,
    cohort_schema = "main",
    cohort_table = "cohort"
  )

  # let it run without stopping as test is later
  testthat::expect_error(
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    ),
    NA
  )

  eras <- DatabaseConnector::querySql(
    connection = connection,
    sql = "SELECT cohort_definition_id, outcome_washout, subject_id, cohort_start_date, cohort_end_date FROM main.outcome_era ORDER BY subject_id, cohort_start_date"
  )

  expected <- data.frame(
    cohort_definition_id = c(3, 3,3),
    outcome_washout = c(365, 365, 365),
    subject_id = c(1, 1, 2),
    cohort_start_date = as.Date(c("2020-01-01", "2021-07-01","2020-03-01")),
    cohort_end_date = as.Date(c("2020-06-10", "2021-07-10","2020-03-05"))
  )

  testthat::expect_equal(eras, expected)


  # now test rerunning with another washout
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "OutcomeEras.sql",
    packageName = "Characterization",
    dbms = "sqlite",
    tempEmulationSchema = "main",
    characterization_schema = "main",
    outcome_era_table = "outcome_era",
    outcome_ids = "3",
    outcome_washout = 365*10,
    cohort_schema = "main",
    cohort_table = "cohort"
  )

  # let it run without stopping as test is later
  testthat::expect_error(
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    ),
    NA
  )

  eras <- DatabaseConnector::querySql(
    connection = connection,
    sql = "SELECT cohort_definition_id, outcome_washout, subject_id, cohort_start_date, cohort_end_date FROM main.outcome_era ORDER BY subject_id, cohort_start_date"
  )

  expected <- data.frame(
    cohort_definition_id = c(3,3,3,3,3),
    outcome_washout = c(365, 3650, 365, 365,3650),
    subject_id = c(1, 1, 1, 2, 2),
    cohort_start_date = as.Date(c("2020-01-01","2020-01-01", "2021-07-01","2020-03-01", "2020-03-01")),
    cohort_end_date = as.Date(c("2020-06-10","2021-07-10", "2021-07-10","2020-03-05", "2020-03-05"))
  )

  testthat::expect_equal(eras, expected)


})


test_that("getCohortJobs", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)

  timeToEventSettings1 <- createTimeToEventSettings(
    createStudyPopulationSettings(
      targetIds = 1
    ),
    outcomeIds = c(3, 4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    createStudyPopulationSettings(
      targetIds = 2
    ),
    outcomeIds = c(3, 4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  targetBaselineSettings1 <- createTargetBaselineSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE
    )
  )

  targetBaselineSettings2 <- createTargetBaselineSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsAge = TRUE,
      useDemographicsRace = TRUE
    )
  )

  riskFactorSettings <- createRiskFactorSettings(
    createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 365,
      minPriorObservation = 365
    ),
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE,
      useDemographicsAge = TRUE,
      useDemographicsRace = TRUE
    )
  )

  caseSeriesSettings <- createCaseSeriesSettings(
    createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 365,
      minPriorObservation = 365
    ),
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    caseCovariateSettings = createDuringCovariateSettings(
      useVisitCountDuring = TRUE,
      useConditionOccurrenceDuring = TRUE
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
    targetBaselineSettings = list(
      targetBaselineSettings1,
      targetBaselineSettings2
    ),
    riskFactorSettings = list(riskFactorSettings),
    caseSeriesSettings = list(caseSeriesSettings)
  )


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 1
  )

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 3)

testthat::expect_true(sum(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds %in% targetIds) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 2
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 6)

testthat::expect_true(sum(unique(c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds, ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 3
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 9)

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
  ) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 4
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 9)

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
) == 3)



jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'CohortIncidence',
  nTargetJobs = 4
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == (12 + 1)) # one more due to outcome eras

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'PatientLevelPrediction',
  nTargetJobs = 4
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == (12 + 1)) # 1 extra outcome era

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
) == 3)

})
