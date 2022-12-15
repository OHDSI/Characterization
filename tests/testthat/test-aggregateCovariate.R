#library(Characterization)
#library(testthat)

context('AggregateCovariate')

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
    riskWindowStart = 1, startAnchor = 'cohort start',
    riskWindowEnd = 365, endAnchor = 'cohort start',
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

  targetIds <- c(1,2,4)
  outcomeIds <- c(3)
  covariateSettings <- FeatureExtraction::createCovariateSettings(
    useDemographicsGender = T,
    useDemographicsAge = T,
    useCharlsonIndex = T
  )

  res <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1, startAnchor = 'cohort start',
    riskWindowEnd = 5*365, endAnchor = 'cohort start',
    covariateSettings = covariateSettings
  )

  agc <- computeAggregateCovariateAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = 'main',
    cdmVersion = 5,
    targetDatabaseSchema = 'main',
    targetTable = 'cohort',
    aggregateCovariateSettings = res
    )


  testthat::expect_true(inherits(agc, 'CovariateData'))
  testthat::expect_true(length(unique(as.data.frame(agc$covariates)$cohortDefinitionId))
                        <= length(res$targetIds)*length(res$outcomeIds)*3+length(res$targetIds)+length(res$outcomeIds))
  testthat::expect_true(
    sum(names(agc) %in% c(
      "analysisRef",
      "covariateRef",
      "covariates",
      "covariatesContinuous"
    )
    ) == 4
  )

  # test saving/loading
  if(actions){ # causing *** caught segfault *** address 0x68, cause 'memory not mapped' zip::zipr
  saveAggregateCovariateAnalyses(
    result = agc,
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_true(
    dir.exists(file.path(tempdir(), 'AggregateCovariate'))
  )

  agc2 <- loadAggregateCovariateAnalyses(
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_equal(nrow(as.data.frame(agc$covariates)), nrow(agc2$covariates))
  testthat::expect_equal(nrow(as.data.frame(agc$covariatesContinuous)), nrow(agc2$covariatesContinuous))
}

  # test exporting to csv
  fileLocs <- exportAggregateCovariateToCsv(
    result = agc,
    saveDirectory = tempdir()
  )

  for(i in 1:length(fileLocs)){
    testthat::expect_true(file.exists(fileLocs[i]))
  }

})


