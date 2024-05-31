# library(Characterization)
# library(testthat)

context("AggregateCovariate")

test_that("createAggregateCovariateSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = T,
    useDemographicsAge = T,
    useCharlsonIndex = T
  )

  caseCovariateSettings <- createDuringCovariateSettings(
    useConditionOccurrenceDuring = T
  )

  res <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = 10,
    outcomeWashoutDays = 100,
    riskWindowStart = 2, startAnchor = "cohort end",
    riskWindowEnd = 363, endAnchor = "cohort end",
    covariateSettings = covariateSettings,
    caseCovariateSettings = caseCovariateSettings,
    casePreTargetDuration = 180,
    casePostOutcomeDuration = 120
  )

  testthat::expect_equal(
    res$targetIds,
    targetIds
  )
  testthat::expect_equal(
    res$covariateSettings[[1]],
    covariateSettings
  )

  testthat::expect_equal(
    res$minPriorObservation,
    10
  )

  testthat::expect_equal(
    res$outcomeWashoutDays,
    100
  )

  testthat::expect_equal(
    res$riskWindowStart,2
  )
  testthat::expect_equal(
    res$startAnchor, "cohort end"
  )
  testthat::expect_equal(
    res$riskWindowEnd,363
  )
  testthat::expect_equal(
    res$endAnchor,"cohort end"
  )

  testthat::expect_equal(
    res$caseCovariateSettings,
    caseCovariateSettings
  )

  testthat::expect_equal(
    res$casePreTargetDuration,
    180
  )

  testthat::expect_equal(
    res$casePostOutcomeDuration,
    120
  )


})

test_that("error when using temporal features", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  temporalCovariateSettings <- FeatureExtraction::createDefaultTemporalCovariateSettings()

  testthat::expect_error(
    createAggregateCovariateSettings(
      targetIds = targetIds,
      outcomeIds = outcomeIds,
      minPriorObservation = 10,
      outcomeWashoutDays = 100,
      riskWindowStart = 1, startAnchor = "cohort start",
      riskWindowEnd = 365, endAnchor = "cohort start",
      covariateSettings = temporalCovariateSettings,
      minCharacterizationMean = 0.01
    )
  )

  temporalCovariateSettings <- list(
    FeatureExtraction::createDefaultCovariateSettings(),
    FeatureExtraction::createDefaultTemporalCovariateSettings()
  )

  testthat::expect_error(
    createAggregateCovariateSettings(
      targetIds = targetIds,
      outcomeIds = outcomeIds,
      minPriorObservation = 10,
      outcomeWashoutDays = 100,
      riskWindowStart = 1, startAnchor = "cohort start",
      riskWindowEnd = 365, endAnchor = "cohort start",
      covariateSettings = temporalCovariateSettings,
      minCharacterizationMean = 0.01
    )
  )

})

test_that("createAggregateCovariateSettingsList", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))
  covariateSettings1 <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = T,
    useDemographicsAge = T,
    useCharlsonIndex = T
  )
  covariateSettings2 <- FeatureExtraction::createCovariateSettings(
    useConditionOccurrenceAnyTimePrior = TRUE
  )
  covariateSettings <- list(covariateSettings1, covariateSettings2)

  res <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1, startAnchor = "cohort start",
    riskWindowEnd = 365, endAnchor = "cohort start",
    covariateSettings = covariateSettings
  )

  testthat::expect_equal(
    res$targetIds,
    targetIds
  )
  testthat::expect_equal(
    res$covariateSettings,
    covariateSettings
  )
})

test_that("computeAggregateCovariateAnalyses", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = T,
    useDemographicsAge = T,
    useCharlsonIndex = T
  )
  caseCovariateSettings <- createDuringCovariateSettings(
    useConditionOccurrenceDuring = T
  )

  res <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = 30,
    outcomeWashoutDays = 1,
    riskWindowStart = 1, startAnchor = "cohort start",
    riskWindowEnd = 5 * 365, endAnchor = "cohort start",
    covariateSettings = covariateSettings,
    caseCovariateSettings = caseCovariateSettings
  )

  tempFolder1 <- tempfile("runAggregate1")
  on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)

  computeAggregateCovariateAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    aggregateCovariateSettings = res,
    minCharacterizationMean = 0.01,
    databaseId = 'madeup',
    outputFolder = tempFolder1
  )
  # check incremental does not run
  testthat::expect_true(sum(c('results','execution') %in% dir(tempFolder1)) == length(dir(tempFolder1)))

  tempFolder2 <- tempfile("runAggregate2")
  on.exit(unlink(tempFolder2, recursive = TRUE), add = TRUE)

  computeAggregateCovariateAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    aggregateCovariateSettings = res,
    minCharacterizationMean = 0.01,
    databaseId = 'madeup',
    outputFolder = tempFolder2,
    incrementalFile = file.path(tempFolder2,'executed.csv')
  )
  # check incremental does run
  testthat::expect_true(sum(c('executed.csv','results','execution') %in% dir(tempFolder2)) == 3)

  # make sure the execution logs all the completed runs
  executed <- readr::read_csv(file.path(tempFolder2,'executed.csv'), show_col_types = F)
  nrowExpected <- length(targetIds)*2 + length(outcomeIds)*2 + 4*length(targetIds)*length(outcomeIds)
  testthat::expect_true(nrow(executed) == nrowExpected)

  # check one execution folder where we have all results
  resultFiles <- dir(file.path(tempFolder2, 'execution', 'T_1_30'))
  testthat::expect_true(
    sum(resultFiles %in% c(
      "analysis_ref.csv",
      "covariate_ref.csv",
      "covariates.csv",
      "covariates_continuous.csv"
    )) == 4
  )

  # make sure the files are written
  resultFiles <- dir(file.path(tempFolder2, 'results'))
  testthat::expect_true(
    sum(resultFiles %in% c(
      "analysis_ref.csv",
      "covariate_ref.csv",
      "covariates.csv",
      "covariates_continuous.csv",
      "settings.csv",
      "cohort_details.csv"
    )) == 6
  )

  # check cohortCounts is done for all
  cohortDetails <- readr::read_csv(
    file.path(tempFolder2, 'results', 'cohort_details.csv'),
    show_col_types = F
  )
  testthat::expect_true(
    nrow(unique(cohortDetails)) == nrow(cohortDetails)
  )
  testthat::expect_true(
    nrow(executed) == nrow(cohortDetails)
  )

  testthat::expect_true(
    nrow(as.data.frame(cohortDetails)) == 20 # 8 T/Os, 3 TnO, 0 TnOc, 3 OnT, 3 TnOprior, 3 TnObetween
  )

  # test results combine execution files correctly
  executionFolders <- dir(file.path(tempFolder2, 'execution'))
  allcovs <- c()
  for(executionFolder in executionFolders){
    covstemp <- readr::read_csv(
      file = file.path(
        tempFolder2,
        'execution',
        executionFolder,
        'covariates.csv'
        ),
      show_col_types = F
    )
    allcovs <- rbind(covstemp, allcovs)
  }

  aggCovs <-  readr::read_csv(
    file = file.path(tempFolder2, 'results', 'covariates.csv'),
    show_col_types = F
  )
  testthat::expect_true(
    nrow(aggCovs) == nrow(allcovs)
  )

  # check covariates is unique
  testthat::expect_true(
    nrow(aggCovs) == nrow(unique(aggCovs))
  )

  # check databaseId is added
  testthat::expect_true(
    aggCovs$database_id[1] == 'madeup'
  )

})
