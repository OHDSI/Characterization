# library(Characterization)
# library(testthat)

context("AggregateCovariate")

tempFolder1 <- tempfile("runAggregate1")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)
tempFolder2 <- tempfile("runAggregate2")
on.exit(unlink(tempFolder1, recursive = TRUE), add = TRUE)


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

test_that("createExecutionIds", {
  testIds <- createExecutionIds(10)
  testthat::expect_true(length(testIds) == 10)
  testthat::expect_true(length(unique(testIds)) == 10)

  testId1 <-  createExecutionIds(1)
  testId2 <-  createExecutionIds(1)
  testthat::expect_true(testId1 != testId2)
})


test_that("getAggregateCovariatesJobs", {
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

  minPriorObservation <- sample(30,1)

  res <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = minPriorObservation,
    outcomeWashoutDays = 1,
    riskWindowStart = 1, startAnchor = "cohort start",
    riskWindowEnd = 5 * 365, endAnchor = "cohort start",
    covariateSettings = covariateSettings,
    caseCovariateSettings = caseCovariateSettings
  )

  jobDf <- getAggregateCovariatesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      aggregateCovariateSettings = res
      ),
    threads = 1
  )

  testthat::expect_true(
    sum(c('computeTargetAggregateCovariateAnalyses',
      'computeCaseAggregateCovariateAnalyses') %in%
      jobDf$functionName) == 2
    )
  testthat::expect_true(nrow(jobDf) == 2)

  testthat::expect_true(
    paste0('tac_1_',minPriorObservation) %in% jobDf$executionFolder
      )
  testthat::expect_true(
    paste0('cac_1_',minPriorObservation, '_1_365_365') %in% jobDf$executionFolder
  )

  settings <- ParallelLogger::convertJsonToSettings(jobDf$settings[1])
  covSettings <- ParallelLogger::convertJsonToSettings(settings$covariateSettingsJson)
  testthat::expect_true(
    covSettings[[1]]$DemographicsGender == T
  )
  testthat::expect_true(
    covSettings[[1]]$CharlsonIndex == T
  )
  testthat::expect_true(
    covSettings[[1]]$DemographicsAge == T
  )


  # now check threads = 2
  jobDf <- getAggregateCovariatesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      aggregateCovariateSettings = res
    ),
    threads = 2
  )
  testthat::expect_true(nrow(jobDf) == 4)

  testthat::expect_true(
    sum(c(paste0('tac_1_',minPriorObservation),
      paste0('tac_2_',minPriorObservation))
      %in% jobDf$executionFolder ) == 2
  )
  testthat::expect_true(
    sum(c(paste0('cac_1_',minPriorObservation, '_1_365_365'),
          paste0('cac_2_',minPriorObservation, '_1_365_365'))
        %in% jobDf$executionFolder) == 2
  )

  # now check threads = 3
  jobDf <- getAggregateCovariatesJobs(
    characterizationSettings = Characterization::createCharacterizationSettings(
      aggregateCovariateSettings = res
    ),
    threads = 3
  )
  testthat::expect_true(nrow(jobDf) == 2*3)

  # now check threads = 4
  jobDf <- getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = res
    ),
    threads = 4
  )
  testthat::expect_true(nrow(jobDf) == 7)

  # now check threads = 5
  jobDf <- getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = res
    ),
    threads = 5
  )
  testthat::expect_true(nrow(jobDf) == 7)

  testthat::expect_true(
    length(unique(unlist(lapply(1:nrow(jobDf),
         function(i){
          ParallelLogger::convertJsonToSettings(jobDf$settings[i])$settingId
         }
  )))) == 2)


  # add more settings
  res2 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = minPriorObservation + 1,
    outcomeWashoutDays = 100,
    riskWindowStart = 1, startAnchor = "cohort start",
    riskWindowEnd = 5 * 365, endAnchor = "cohort start",
    covariateSettings = covariateSettings,
    caseCovariateSettings = caseCovariateSettings
  )

  jobDf <-  getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = list(res, res2)
    ),
    threads = 1
  )
  testthat::expect_true(nrow(jobDf) == 4)
  testthat::expect_true(
    length(unique(unlist(lapply(1:nrow(jobDf),
                                function(i){
                                  ParallelLogger::convertJsonToSettings(jobDf$settings[i])$settingId
                                }
    )))) == 4)

  jobDf <- getAggregateCovariatesJobs(
      characterizationSettings = createCharacterizationSettings(
        aggregateCovariateSettings = list(res, res2)
      ),
      threads = 3
    )
  testthat::expect_true(nrow(jobDf) == 12)
  testthat::expect_true(
    length(unique(unlist(lapply(1:nrow(jobDf),
                                function(i){
                                  ParallelLogger::convertJsonToSettings(jobDf$settings[i])$settingId
                                }
    )))) == 4)


  # test when extractNonCaseCovariates = F
  res3 <- Characterization:::createAggregateCovariateSettings(
    targetIds = 1,
    outcomeIds = 3,
    extractNonCaseCovariates = F
    )
  jobDf <- Characterization:::getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = list(res3)
    ),
    threads = 3
  )
  testthat::expect_true(nrow(jobDf) == 1)

  res4 <- Characterization:::createAggregateCovariateSettings(
    targetIds = 2,
    outcomeIds = 3,
    extractNonCaseCovariates = T
  )
  jobDf <- Characterization:::getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = list(res3, res4)
    ),
    threads = 3
  )

  # add checks

})

test_that("computeTargetAggregateCovariateAnalyses", {
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

  jobDf <-  getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = res
      ),
    threads = 1
  )

  computeTargetAggregateCovariateAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = ParallelLogger::convertJsonToSettings(jobDf$settings[1]),
    minCharacterizationMean = 0.01,
    databaseId = 'madeup',
    outputFolder = tempFolder1
  )
  # check incremental does not run
  testthat::expect_true(
    sum(c('cohort_details.csv',
          'settings.csv',
          'covariates.csv',
          'covariates_continuous.csv',
          'cohort_counts.csv',
          'covariate_ref.csv',
          'analysis_ref.csv'
    ) %in% dir(tempFolder1)
    ) == length(dir(tempFolder1))
  )

  # check cohortCounts is done for all
  cohortDetails <- readr::read_csv(
    file.path(tempFolder1,'cohort_details.csv'),
    show_col_types = F
  )
  testthat::expect_true(
    nrow(unique(cohortDetails)) == nrow(cohortDetails)
  )
  testthat::expect_true(
    nrow(cohortDetails) == 8
  )

  aggCovs <-  readr::read_csv(
    file = file.path(tempFolder1, 'covariates.csv'),
    show_col_types = F
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


test_that("computeCaseAggregateCovariateAnalyses", {
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

  jobDf <- getAggregateCovariatesJobs(
    characterizationSettings = createCharacterizationSettings(
      aggregateCovariateSettings = res
      ),
    threads = 1
  )

  computeCaseAggregateCovariateAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = ParallelLogger::convertJsonToSettings(jobDf$settings[2]),
    minCharacterizationMean = 0.01,
    databaseId = 'madeup',
    outputFolder = tempFolder2
  )
  # check incremental does not run
  testthat::expect_true(
    sum(c('cohort_details.csv',
          'settings.csv',
          'covariates.csv',
          'covariates_continuous.csv',
          'cohort_counts.csv',
          'covariate_ref.csv',
          'analysis_ref.csv'
    ) %in% dir(tempFolder2)
    ) == length(dir(tempFolder2))
  )

  # check cohortCounts is done for all
  cohortDetails <- readr::read_csv(
    file.path(tempFolder2,'cohort_details.csv'),
    show_col_types = F
  )
  testthat::expect_true(
    nrow(unique(cohortDetails)) == nrow(cohortDetails)
  )
  testthat::expect_true(
    nrow(cohortDetails) == 3*5
  )

  aggCovs <-  readr::read_csv(
    file = file.path(tempFolder2, 'covariates.csv'),
    show_col_types = F
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
