# library(Characterization)
# library(testthat)

context("CaseSeries")

tempFolder1 <- tempfile("runCs1")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)
tempFolder2 <- tempfile("runCs2")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)


test_that("createCaseSeriesSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings <- Characterization::createDuringCovariateSettings(
    useDrugEraDuring = TRUE,
    useVisitCountDuring = TRUE
    )

  res <- createCaseSeriesSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    limitToFirstInNDays = 9,
    minPriorObservation = 10,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 0,
    endAnchor = 'cohort end',
    caseCovariateSettings = covariateSettings,
    casePreTargetDuration = 180,
    casePostOutcomeDuration = 181
  )

  testthat::expect_equal(
    res$targetIds,
    targetIds
  )
  testthat::expect_equal(
    res$caseCovariateSettings[[1]],
    covariateSettings
  )

  testthat::expect_equal(
    res$minPriorObservation,
    10
  )

  testthat::expect_equal(
    res$limitToFirstInNDays,
    9
  )

  testthat::expect_equal(
    res$outcomeWashoutDays,
    365
  )

  testthat::expect_equal(
    res$riskWindowStart,
    1
  )

  testthat::expect_equal(
    res$startAnchor,
    'cohort_start'
  )

  testthat::expect_equal(
    res$riskWindowEnd,
    0
  )

  testthat::expect_equal(
    res$endAnchor,
    'cohort_end'
  )

  testthat::expect_equal(
    res$casePreTargetDuration,
    180
  )
  testthat::expect_equal(
    res$casePostOutcomeDuration,
    181
  )

})

test_that("error when using temporal features - risk factors", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  temporalCovariateSettings <- FeatureExtraction::createDefaultTemporalCovariateSettings()

  testthat::expect_error(
    res <- createCaseSeriesSettings(
      targetIds = targetIds,
      outcomeIds = outcomeIds,
      limitToFirstInNDays = 9,
      minPriorObservation = 10,
      outcomeWashoutDays = 365,
      riskWindowStart = 1,
      startAnchor = 'cohort start',
      riskWindowEnd = 0,
      endAnchor = 'cohort end',
      caseCovariateSettings = temporalCovariateSettings
    )
  )

  temporalCovariateSettings <- list(
    FeatureExtraction::createDefaultCovariateSettings(),
    FeatureExtraction::createDefaultTemporalCovariateSettings()
  )

  testthat::expect_error(
    res <- createCaseSeriesSettings(
      targetIds = targetIds,
      outcomeIds = outcomeIds,
      limitToFirstInNDays = 9,
      minPriorObservation = 10,
      outcomeWashoutDays = 365,
      riskWindowStart = 1,
      startAnchor = 'cohort start',
      riskWindowEnd = 0,
      endAnchor = 'cohort end',
      covariateSettings = temporalCovariateSettings
    )
  )
})

test_that("createCaseSeriesSettings covariateList", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings1 <- Characterization::createDuringCovariateSettings(
    useVisitCountDuring = T
  )
  covariateSettings2 <- Characterization::createDuringCovariateSettings(
    useConditionOccurrenceDuring = TRUE
  )
  covariateSettings <- list(covariateSettings1, covariateSettings2)

  res <- createCaseSeriesSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    limitToFirstInNDays = 9,
    minPriorObservation = 10,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 0,
    endAnchor = 'cohort end',
    caseCovariateSettings = covariateSettings
  )

  testthat::expect_equal(
    res$targetIds,
    targetIds
  )
  testthat::expect_equal(
    res$caseCovariateSettings,
    covariateSettings
  )
})


test_that("getCaseSeriesJobs", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)
  covariateSettings <- Characterization::createDuringCovariateSettings(
    useDrugEraDuring = TRUE,
    useVisitCountDuring = TRUE
  )

  minPriorObservation <- sample(30, 1)
  limitToFirstInNDays <- sample(300, 1)

  res <- createCaseSeriesSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = minPriorObservation,
    limitToFirstInNDays = limitToFirstInNDays,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 0,
    endAnchor = 'cohort end',
    caseCovariateSettings = covariateSettings
  )

  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 1
  )

  testthat::expect_true(
    unique(jobDf$functionName) == "computeCaseSeriesAnalyses"
  )
  testthat::expect_true(nrow(jobDf) == 1)

  settings <- ParallelLogger::convertJsonToSettings(jobDf$settings[1])
  covSettings <- ParallelLogger::convertJsonToSettings(settings$covariateSettingsJson)
  testthat::expect_true(
    covSettings[[1]]$DrugEraDuring == TRUE
  )
  testthat::expect_true(
    covSettings[[1]]$VisitCountDuring == TRUE
  )


  # now check nTargetJobs = 2
  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 2
  )
  testthat::expect_true(nrow(jobDf) == 2)

  # now check nTargetJobs = 3
  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 3
  )
  testthat::expect_true(nrow(jobDf) == 3)

  # check the target ids
  testthat::expect_true(ParallelLogger::convertJsonToSettings(jobDf$settings[1])$targetIds == targetIds[1])
  testthat::expect_true(ParallelLogger::convertJsonToSettings(jobDf$settings[2])$targetIds == targetIds[2])
  testthat::expect_true(ParallelLogger::convertJsonToSettings(jobDf$settings[3])$targetIds == targetIds[3])

  # now check nTargetJobs = 4
  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 4
  )
  testthat::expect_true(nrow(jobDf) == 3)

  # now check threads = 50
  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 50
  )
  testthat::expect_true(nrow(jobDf) == 3)

})

test_that("computeCaseSeriesAnalyses", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)
  covariateSettings <- Characterization::createDuringCovariateSettings(
    useDrugEraDuring = TRUE,
    useVisitCountDuring = TRUE
  )

  res <- createCaseSeriesSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    limitToFirstInNDays = 365,
    minPriorObservation = 30,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 365,
    endAnchor = 'cohort start',
    caseCovariateSettings = covariateSettings
  )

  jobDf <- getCaseSeriesJobs(
    characterizationSettings = createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 1
  )

  tables <- Characterization:::generateCohorts(
    characterizationSettings = createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    mode = 'PatientLevelPrediction',
    incremental = FALSE,
    executionPath = tempFolder1, # used for incremental logging
    connectionDetails = connectionDetails,

    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    outputDatabaseSchema = 'main',
    outputTable = 'char_cohort',
    cdmDatabaseSchema = "main",
    tempEmulationSchema = "main",
    progressBar = FALSE,
    settingHash = 'set1',
    dbHash = 'db1'
    )

  Characterization:::computeCaseSeriesAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",

    characterizationDatabaseSchema = 'main',
    characterizationTable = tables$characterizationTable, # contains char cohorts
    targetSettingsTable = tables$targetSettingsTable, # contains map between settings and char cohort id
    caseSettingsTable = tables$caseSettingsTable,
    tempEmulationSchema = 'main',
    settings = ParallelLogger::convertJsonToSettings(jobDf$settings[1]),
    databaseId = "madeup",
    outputFolder = tempFolder1,
    minCellCount = 0,
    progressBar = FALSE,
    minCharacterizationMean = 0,
    minCovariateCount = 0,
    executionId = 'execution123',
    minSMD = 0,
    mode = 'PatientLevelPrediction'
  )

  # check incremental does not run
  testthat::expect_true(
    sum(c(
      "case_series_covariates.csv",
      "case_series_covariates_continuous.csv",
      "covariate_ref.csv",
      "analysis_ref.csv"
    ) %in% dir(tempFolder1)) == 4
  )


  covs <- readr::read_csv(
    file = file.path(tempFolder1, "case_series_covariates.csv"),
    show_col_types = FALSE
  )
  # check covariates is unique
  testthat::expect_true(
    nrow(covs) == nrow(unique(covs))
  )

  # check key columns are there
  testthat::expect_true('database_id' %in% colnames(covs))
  testthat::expect_true('setting_id' %in% colnames(covs))
  testthat::expect_true('characterization_case_id' %in% colnames(covs))
  testthat::expect_true('covariate_id' %in% colnames(covs))

  # check databaseId is added
  if(nrow(covs) > 0){
    testthat::expect_true(
      covs$database_id[1] == "madeup"
    )
  }
})

# TODO: add a test to make sure the SMD is correct? could create fake data in sqlite to run
# risk factor extraction sql

