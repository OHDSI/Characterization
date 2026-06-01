context("TargetAnalysis")

tempFolder1 <- tempfile("runTarget1")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)
tempFolder2 <- tempfile("runTarget2")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)


test_that("createTargetBaselineSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )

  res <- createTargetBaselineSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 9,
      minPriorObservation = 10,
    ),
    covariateSettings = covariateSettings
  )

  testthat::expect_equal(
    unique(res$studyPopulationSettings$targetId),
    unique(targetIds)
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

})

test_that("error when using temporal features", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  temporalCovariateSettings <- FeatureExtraction::createDefaultTemporalCovariateSettings()

  testthat::expect_error(
    createTargetBaselineSettings(
      studyPopulationSettings = createStudyPopulationSettings(
        targetIds = targetIds,
        limitToFirstInNDays = 9,
        minPriorObservation = 10,
      ),
      covariateSettings = temporalCovariateSettings
    )
  )

  temporalCovariateSettings <- list(
    FeatureExtraction::createDefaultCovariateSettings(),
    FeatureExtraction::createDefaultTemporalCovariateSettings()
  )

  testthat::expect_error(
    createTargetBaselineSettings(
      studyPopulationSettings = createStudyPopulationSettings(
        targetIds = targetIds,
        limitToFirstInNDays = 9,
        minPriorObservation = 10,
      ),
      covariateSettings = temporalCovariateSettings
    )
  )
})

test_that("createTargetBaselineSettings covariateList", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings1 <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )
  covariateSettings2 <- FeatureExtraction::createCovariateSettings(
    useConditionOccurrenceAnyTimePrior = TRUE
  )
  covariateSettings <- list(covariateSettings1, covariateSettings2)

  res <- createTargetBaselineSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds
    ),
    covariateSettings = covariateSettings
  )

  testthat::expect_equal(
    unique(res$studyPopulationSettings$targetId),
    unique(targetIds)
  )
  testthat::expect_equal(
    res$covariateSettings,
    covariateSettings
  )
})


test_that("getTargetBaselineJobs", {
  targetIds <- c(1, 2, 4)
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )

  minPriorObservation <- sample(30, 1)
  limitToFirstInNDays <- sample(300, 1)

  studyPop <- createStudyPopulationSettings(
    targetIds = targetIds,
    minPriorObservation = minPriorObservation,
    limitToFirstInNDays = limitToFirstInNDays
  )

  res <- createTargetBaselineSettings(
    studyPopulationSettings = studyPop,
    covariateSettings = covariateSettings
  )

  jobDf <-  getTargetBaselineJobs(
    characterizationSettings = createCharacterizationSettings(
      targetBaselineSettings = res
    ),
    nTargetJobs = 1
  )

  testthat::expect_true(
    unique(jobDf$functionName) == "computeTargetBaselineAnalyses"
  )
  testthat::expect_true(nrow(jobDf) == 1)

  testthat::expect_true(
    paste("t_1", collapse ='_') %in% jobDf$executionFolder
  )

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
  jobDf <-  getTargetBaselineJobs(
    characterizationSettings = createCharacterizationSettings(
      targetBaselineSettings = res
    ),
    nTargetJobs = 2
  )
  testthat::expect_true(nrow(jobDf) == 2)

  testthat::expect_true(
    sum(c(
      paste0("t_1", collapse = '_'),
      paste0("t_2", collapse = '_')
    )
    %in% jobDf$executionFolder) == 2
  )

  # now check nTargetJobs = 3
  charSettings <- createCharacterizationSettings(
    targetBaselineSettings = res
  )
  jobDf <- getTargetBaselineJobs(
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
  jobDf <- getTargetBaselineJobs(
    characterizationSettings = createCharacterizationSettings(
      targetBaselineSettings = res
    ),
    nTargetJobs = 4
  )
  testthat::expect_true(nrow(jobDf) == 3)

  # now check threads = 50
  jobDf <- getTargetBaselineJobs(
    characterizationSettings = createCharacterizationSettings(
      targetBaselineSettings = res
    ),
    nTargetJobs = 50
  )
  testthat::expect_true(nrow(jobDf) == 3)

})

test_that("computeTargetBaselineAnalyses", {
  targetIds <- c(1, 2, 4)
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useCharlsonIndex = TRUE
  )

  res <- createTargetBaselineSettings(
    createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 365,
      minPriorObservation = 30
    ),
    covariateSettings = covariateSettings
  )

  jobDf <- getTargetBaselineJobs(
    characterizationSettings = createCharacterizationSettings(
      targetBaselineSettings = res
    ),
    nTargetJobs = 1
  )

  tables <- generateCohorts(
    characterizationSettings = createCharacterizationSettings(
      targetBaselineSettings = res
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

  computeTargetBaselineAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",

    characterizationDatabaseSchema = 'main',
    characterizationTable = tables$characterizationTable, # contains char cohorts
    targetSettingsTable = tables$targetSettingsTable, # contains map between settings and char cohort id
    tempEmulationSchema = 'main',
    settings = ParallelLogger::convertJsonToSettings(jobDf$settings[1]),
    databaseId = "madeup",
    outputFolder = tempFolder1,
    progressBar = FALSE,
    minCharacterizationMean = 0,
    minCovariateCount = 0,
    executionId = 'execution123'
  )

  result <- Andromeda::loadAndromeda(file.path(tempFolder1, "result"))

  # check incremental does not run
  testthat::expect_true(
    sum(c(
      "targetCovariates",
      "targetCovariatesContinuous",
      "covariateRef",
      "analysisRef"
    ) %in% names(result)) == 4
  )


  covs <- as.data.frame(result$targetCovariates)

  # check covariates is unique
  testthat::expect_true(
    nrow(covs) == nrow(unique(covs))
  )

  # check databaseId is added
  testthat::expect_true(
    covs$databaseId[1] == "madeup"
  )
})
