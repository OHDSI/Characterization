# library(Characterization)
# library(testthat)

context("RiskFactor")

tempFolder1 <- tempfile("runRf1")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)
tempFolder2 <- tempfile("runRf2")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)


test_that("createRiskFactorSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )

  res <- createRiskFactorSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 9,
      minPriorObservation = 10
    ),
    outcomeIds = outcomeIds,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 0,
    endAnchor = 'cohort end',
    covariateSettings = covariateSettings
  )

  testthat::expect_equal(
    res$studyPopulationSettings$targetId,
    targetIds
  )
  testthat::expect_equal(
    res$covariateSettings[[1]],
    covariateSettings
  )

  testthat::expect_equal(
    unique(res$studyPopulationSettings$minPriorObservation),
    10
  )

  testthat::expect_equal(
    unique(res$studyPopulationSettings$limitToFirstInNDays),
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

})

test_that("error when using temporal features - risk factors", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  temporalCovariateSettings <- FeatureExtraction::createDefaultTemporalCovariateSettings()

  testthat::expect_error(
    res <- createRiskFactorSettings(
      studyPopulationSettings = createStudyPopulationSettings(
        targetIds = targetIds,
        limitToFirstInNDays = 9,
        minPriorObservation = 10
      ),
      outcomeIds = outcomeIds,
      outcomeWashoutDays = 365,
      riskWindowStart = 1,
      startAnchor = 'cohort start',
      riskWindowEnd = 0,
      endAnchor = 'cohort end',
      covariateSettings = temporalCovariateSettings
    )
  )

  temporalCovariateSettings <- list(
    FeatureExtraction::createDefaultCovariateSettings(),
    FeatureExtraction::createDefaultTemporalCovariateSettings()
  )

  testthat::expect_error(
    res <- createRiskFactorSettings(
      studyPopulationSettings = createStudyPopulationSettings(
        targetIds = targetIds,
        limitToFirstInNDays = 9,
        minPriorObservation = 10
      ),
      outcomeIds = outcomeIds,
      outcomeWashoutDays = 365,
      riskWindowStart = 1,
      startAnchor = 'cohort start',
      riskWindowEnd = 0,
      endAnchor = 'cohort end',
      covariateSettings = temporalCovariateSettings
    )
  )
})

test_that("createRiskFactorSettings covariateList", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings1 <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )
  covariateSettings2 <- FeatureExtraction::createCovariateSettings(
    useConditionOccurrenceAnyTimePrior = TRUE
  )
  covariateSettings <- list(covariateSettings1, covariateSettings2)

  res <- createRiskFactorSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 9,
      minPriorObservation = 10
    ),
    outcomeIds = outcomeIds,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 0,
    endAnchor = 'cohort end',
    covariateSettings = covariateSettings
  )

  testthat::expect_equal(
    res$studyPopulationSettings$targetId,
    targetIds
  )
  testthat::expect_equal(
    res$covariateSettings,
    covariateSettings
  )
})


test_that("getRiskFactorJobs", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )

  minPriorObservation <- sample(30, 1)
  limitToFirstInNDays <- sample(300, 1)

  res <- createRiskFactorSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      minPriorObservation = minPriorObservation,
      limitToFirstInNDays = limitToFirstInNDays
    ),
    outcomeIds = outcomeIds,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 0,
    endAnchor = 'cohort end',
    covariateSettings = covariateSettings
  )

  jobDf <-  getRiskFactorJobs(
    characterizationSettings = createCharacterizationSettings(
      riskFactorSettings = res
    ),
    nTargetJobs = 1
  )

  testthat::expect_true(
    unique(jobDf$functionName) == "computeRiskFactorAnalyses"
  )
  testthat::expect_true(nrow(jobDf) == 1)

  settings <- ParallelLogger::convertJsonToSettings(jobDf$settings[1])
  covSettings <- ParallelLogger::convertJsonToSettings(settings$covariateSettingsJson)
  testthat::expect_true(
    covSettings[[1]]$DemographicsGender == TRUE
  )
  testthat::expect_true(
    covSettings[[1]]$CharlsonIndex == TRUE
  )
  testthat::expect_true(
    covSettings[[1]]$DemographicsAge == TRUE
  )


  # now check nTargetJobs = 2
  jobDf <-  getRiskFactorJobs(
    characterizationSettings = createCharacterizationSettings(
      riskFactorSettings = res
    ),
    nTargetJobs = 2
  )
  testthat::expect_true(nrow(jobDf) == 2)

  # now check nTargetJobs = 3
  charSettings <- createCharacterizationSettings(
    riskFactorSettings = res
  )
  jobDf <-  getRiskFactorJobs(
    characterizationSettings = charSettings,
    nTargetJobs = 3
  )
  testthat::expect_true(nrow(jobDf) == 3)

  # check the target ids
  tId1 <- charSettings$characterizationTargetLookup$characterizationTargetId[
    charSettings$characterizationTargetLookup$targetId == res$studyPopulationSettings$targetId[1]
  ]
  testthat::expect_true(ParallelLogger::convertJsonToSettings(jobDf$settings[1])$characterizationTargetIds == tId1)
  tId2 <- charSettings$characterizationTargetLookup$characterizationTargetId[
    charSettings$characterizationTargetLookup$targetId == res$studyPopulationSettings$targetId[2]
  ]
  testthat::expect_true(ParallelLogger::convertJsonToSettings(jobDf$settings[2])$characterizationTargetIds == tId2)
  tId3 <- charSettings$characterizationTargetLookup$characterizationTargetId[
    charSettings$characterizationTargetLookup$targetId == res$studyPopulationSettings$targetId[3]
  ]
  testthat::expect_true(ParallelLogger::convertJsonToSettings(jobDf$settings[3])$characterizationTargetIds == tId3)

  # now check nTargetJobs = 4
  jobDf <-  getRiskFactorJobs(
    characterizationSettings = createCharacterizationSettings(
      riskFactorSettings = res
    ),
    nTargetJobs = 4
  )
  testthat::expect_true(nrow(jobDf) == 3)

  # now check threads = 50
  jobDf <-  getRiskFactorJobs(
    characterizationSettings = createCharacterizationSettings(
      riskFactorSettings = res
    ),
    nTargetJobs = 50
  )
  testthat::expect_true(nrow(jobDf) == 3)

})

test_that("computeRiskFactorAnalyses", {
  skipIfCreateTargetCohortSqlUnavailable()

  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )

  res <- createRiskFactorSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 365,
      minPriorObservation = 30
    ),
    outcomeIds = outcomeIds,
    outcomeWashoutDays = 365,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 365,
    endAnchor = 'cohort end',
    covariateSettings = covariateSettings
  )

  jobDf <- getRiskFactorJobs(
    characterizationSettings = createCharacterizationSettings(
      riskFactorSettings = res
    ),
    nTargetJobs = 1
  )

  tables <- generateCohorts(
    characterizationSettings = createCharacterizationSettings(
      riskFactorSettings = res
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

  computeRiskFactorAnalyses(
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
    caseCountTable = , tables$caseCountTable,
    minCaseSize = 0,
    tempEmulationSchema = 'main',
    settings = ParallelLogger::convertJsonToSettings(jobDf$settings[1]),
    databaseId = "madeup",
    outputFolder = tempFolder1,
    progressBar = FALSE,
    minCharacterizationMean = 0,
    minCovariateCount = 0,
    executionId = 'execution123',
    minSMD = 0,
    mode = 'PatientLevelPrediction'
  )

  result <- Andromeda::loadAndromeda(file.path(tempFolder1, "result"))

  # check incremental does not run
  testthat::expect_true(
    sum(c(
      "riskFactorCovariates",
      "riskFactorCovariatesContinuous",
      "covariateRef",
      "analysisRef"
    ) %in% names(result)) == 4
  )


  covs <- as.data.frame(result$riskFactorCovariates)

  # check covariates is unique
  testthat::expect_true(
    nrow(covs) == nrow(unique(covs))
  )

  # check key columns are there
  testthat::expect_true('databaseId' %in% colnames(covs))
  testthat::expect_true('settingId' %in% colnames(covs))
  testthat::expect_true('characterizationCaseId' %in% colnames(covs))
  testthat::expect_true('covariateId' %in% colnames(covs))
  testthat::expect_true('standardizedMeanDifference' %in% colnames(covs))

  # check databaseId is added
  if(nrow(covs) > 0){
    testthat::expect_true(
      covs$databaseId[1] == "madeup"
    )
  }
})

