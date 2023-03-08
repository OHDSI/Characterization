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
  covariateSetting <- list(covariateSettings1, covariateSettings2)

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

  res <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1, startAnchor = "cohort start",
    riskWindowEnd = 5 * 365, endAnchor = "cohort start",
    covariateSettings = covariateSettings
  )

  agc <- computeAggregateCovariateAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    cdmVersion = 5,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    aggregateCovariateSettings = res
  )


  testthat::expect_true(inherits(agc, "CovariateData"))
  testthat::expect_true(length(unique(as.data.frame(agc$covariates)$cohortDefinitionId))
  <= length(res$targetIds) * length(res$outcomeIds) * 3 + length(res$targetIds) + length(res$outcomeIds))
  testthat::expect_true(
    sum(names(agc) %in% c(
      "analysisRef",
      "covariateRef",
      "covariates",
      "covariatesContinuous",
      "settings",
      "cohortDetails"
    )) == 6
  )

  # check cohortDetails
  testthat::expect_true(
    length(unique(as.data.frame(agc$cohortDetails)$cohortDefinitionId)) ==
      nrow(as.data.frame(agc$cohortDetails))
  )

  testthat::expect_true(
    nrow(as.data.frame(agc$cohortDetails)) == 13 # 4 T/Os, 3 TnO, 3 TnOc, 3 OnT
  )

  # test saving/loading
  tempFile <- tempfile(fileext = ".zip")
  on.exit(unlink(tempFile))

  saveAggregateCovariateAnalyses(
    result = agc,
    fileName = tempFile
  )

  testthat::expect_true(
    file.exists(tempFile)
  )

  agc2 <- loadAggregateCovariateAnalyses(
    fileName = tempFile
  )

  testthat::expect_equal(dplyr::collect(agc$covariates), dplyr::collect(agc2$covariates))
  testthat::expect_equal(dplyr::collect(agc$covariatesContinuous), dplyr::collect(agc2$covariatesContinuous))

  # test exporting to csv
  tempFolder <- tempfile("exportToCsv")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)
  fileLocs <- exportAggregateCovariateToCsv(
    result = agc,
    saveDirectory = tempFolder
  )

  for (i in 1:length(fileLocs)) {
    testthat::expect_true(file.exists(fileLocs[i]))
  }
})
