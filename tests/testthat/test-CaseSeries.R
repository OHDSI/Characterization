context("CaseSeries")

tempFolder1 <- tempfile("runCs1")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)
tempFolder2 <- tempfile("runCs2")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)


test_that("createCaseSeriesSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings <- createDuringCovariateSettings(
    useDrugEraDuring = TRUE,
    useVisitCountDuring = TRUE
    )

  res <- createCaseSeriesSettings(
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
    caseCovariateSettings = covariateSettings,
    casePreTargetDuration = 180,
    casePostOutcomeDuration = 181
  )

  testthat::expect_equal(
    res$studyPopulationSettings$targetId,
    targetIds
  )
  testthat::expect_equal(
    res$caseCovariateSettings[[1]],
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
      caseCovariateSettings = temporalCovariateSettings
    )
  )

  temporalCovariateSettings <- list(
    FeatureExtraction::createDefaultCovariateSettings(),
    FeatureExtraction::createDefaultTemporalCovariateSettings()
  )

  testthat::expect_error(
    res <- createCaseSeriesSettings(
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

test_that("createCaseSeriesSettings covariateList", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings1 <- createDuringCovariateSettings(
    useVisitCountDuring = T
  )
  covariateSettings2 <- createDuringCovariateSettings(
    useConditionOccurrenceDuring = TRUE
  )
  covariateSettings <- list(covariateSettings1, covariateSettings2)

  res <- createCaseSeriesSettings(
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
    caseCovariateSettings = covariateSettings
  )

  testthat::expect_equal(
    res$studyPopulationSettings$targetId,
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
  covariateSettings <- createDuringCovariateSettings(
    useDrugEraDuring = TRUE,
    useVisitCountDuring = TRUE
  )

  minPriorObservation <- sample(30, 1)
  limitToFirstInNDays <- sample(300, 1)

  res <- createCaseSeriesSettings(
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
    caseCovariateSettings = covariateSettings
  )

  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = createCharacterizationSettings(
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
    characterizationSettings = createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 2
  )
  testthat::expect_true(nrow(jobDf) == 2)

  # now check nTargetJobs = 3
  charSettings <- createCharacterizationSettings(
    caseSeriesSettings = res
  )
  jobDf <-  getCaseSeriesJobs(
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
  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 4
  )
  testthat::expect_true(nrow(jobDf) == 3)

  # now check threads = 50
  jobDf <-  getCaseSeriesJobs(
    characterizationSettings = createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 50
  )
  testthat::expect_true(nrow(jobDf) == 3)

})

test_that("computeCaseSeriesAnalyses", {
  skipIfCreateTargetCohortSqlUnavailable()

  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)
  covariateSettings <- createDuringCovariateSettings(
    useDrugEraDuring = TRUE,
    useVisitCountDuring = TRUE
  )

  res <- createCaseSeriesSettings(
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
    endAnchor = 'cohort start',
    caseCovariateSettings = covariateSettings
  )

  jobDf <- getCaseSeriesJobs(
    characterizationSettings = createCharacterizationSettings(
      caseSeriesSettings = res
    ),
    nTargetJobs = 1
  )

  tables <- generateCohorts(
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

  computeCaseSeriesAnalyses(
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
    caseCountTable = tables$caseCountTable,
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

  # cleanup
  dropCohorts(
    connectionDetails = connectionDetails,
    outputDatabaseSchema = 'main',
    outputTable = 'char_cohort',
    cdmDatabaseSchema = "main",
    tempEmulationSchema = "main",
    progressBar = FALSE,
    settingHash = 'set1',
    dbHash = 'db1'
  )

  # check incremental does not run
  testthat::expect_true(
    'result' %in% dir(tempFolder1)
  )

  res <- Andromeda::loadAndromeda(file.path(tempFolder1, 'result'))

  testthat::expect_true(
    sum(c(
      "caseSeriesCovariates",
      "caseSeriesCovariatesContinuous",
      "covariateRef",
      "analysisRef"
    ) %in% names(res)) == 4
  )


  covs <- as.data.frame(res$caseSeriesCovariates)
  # check covariates is unique
  testthat::expect_true(
    nrow(covs) == nrow(unique(covs))
  )

  # check key columns are there
  testthat::expect_true('databaseId' %in% colnames(covs))
  testthat::expect_true('settingId' %in% colnames(covs))
  testthat::expect_true('characterizationCaseId' %in% colnames(covs))
  testthat::expect_true('covariateId' %in% colnames(covs))

  # check databaseId is added
  if(nrow(covs) > 0){
    testthat::expect_true(
      covs$databaseId[1] == "madeup"
    )
  }
})


# testing case series include/exclude covs
test_that("testing during covs", {
skipIfCreateTargetCohortSqlUnavailable()

targetIds <- c(1, 2, 4)
outcomeIds <- c(3)

res <- createCaseSeriesSettings(
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
  endAnchor = 'cohort start'
)

tables <- generateCohorts(
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

# 2) check excluded
# ======================
covariateSettings1 <- createDuringCovariateSettings(
  useDrugEraDuring = TRUE,
  useVisitCountDuring = TRUE,
  excludedCovariateConceptIds = c(1124300,1118084)
)

data1 <- FeatureExtraction::getDbCovariateData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortTable = 'char_cohort_set1_db1',
  cohortDatabaseSchema = "main",
  cohortIds = c(10,20,40), # the targets
  rowIdField = 'row_id',
  exportToTable = FALSE,
  aggregated = TRUE,
  minCharacterizationMean = 0.01,
  tempEmulationSchema = "main",
  covariateSettings = covariateSettings1
    )

# add checks to make sure data exists
testthat::expect_true(
  nrow(as.data.frame(data1$covariates)) == 0
)

# 2) check included
# ======================
covariateSettings2 <- createDuringCovariateSettings(
  useDrugEraDuring = TRUE,
  includedCovariateIds = c(1124300417)
)

data2 <- FeatureExtraction::getDbCovariateData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortTable = 'char_cohort_set1_db1',
  cohortDatabaseSchema = "main",
  cohortIds = c(10,20,40), # the targets
  rowIdField = 'row_id',
  exportToTable = FALSE,
  aggregated = TRUE,
  minCharacterizationMean = 0.01,
  tempEmulationSchema = "main",
  covariateSettings = covariateSettings2
)

# should only have covariateId 1124300417
testthat::expect_true(
  as.data.frame(data2$covariates)$covariateId == 1124300417
)

covariateSettings3 <- createDuringCovariateSettings(
  useDrugEraDuring = TRUE,
  includedCovariateConceptIds = c(1118084)
)
data3 <- FeatureExtraction::getDbCovariateData(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  cohortTable = 'char_cohort_set1_db1',
  cohortDatabaseSchema = "main",
  cohortIds = c(10,20,40), # the targets
  rowIdField = 'row_id',
  exportToTable = FALSE,
  aggregated = TRUE,
  minCharacterizationMean = 0.01,
  tempEmulationSchema = "main",
  covariateSettings = covariateSettings3
)
# should only have covariateId 1118084417
testthat::expect_true(
  as.data.frame(data3$covariates)$covariateId == 1118084417
)

# cleanup
dropCohorts(
  connectionDetails = connectionDetails,
  outputDatabaseSchema = 'main',
  outputTable = 'char_cohort',
  cdmDatabaseSchema = "main",
  tempEmulationSchema = "main",
  progressBar = FALSE,
  settingHash = 'set1',
  dbHash = 'db1'
)


})

