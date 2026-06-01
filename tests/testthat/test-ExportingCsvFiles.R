context("ExportingCsvFiles")
library(dplyr)

tempFolder <- file.path(tempdir(),"exporting")
on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

tempFolder2 <- file.path(tempdir(),"results")
on.exit(unlink(tempFolder2, recursive = TRUE), add = TRUE)

tempFolder3 <- file.path(tempdir(),"attrition")
on.exit(unlink(tempFolder3, recursive = TRUE), add = TRUE)

test_that("addDbAndSettings works for single table", {

  andromeda <- Andromeda::andromeda()
  andromeda$singleTable <- data.frame(
    column1 = 1:5,
    column2 = rep('empty', 5)
  )

  andromeda <- addDbAndSettings(
    andromeda = andromeda,
    databaseId = 1,
    settingId = 'madeup'
  )

  testthat::expect_true(names(andromeda) == 'singleTable')
  testthat::expect_true(sum(colnames(andromeda$singleTable) %in%
                          c("column1", "column2", "databaseId", "settingId")) == 4)
  testthat::expect_true(unique(as.data.frame(andromeda$singleTable)$databaseId) == 1)
  testthat::expect_true(unique(as.data.frame(andromeda$singleTable)$settingId) == 'madeup')

})


test_that("addDbAndSettings works for multiple tables including empty", {

  andromeda <- Andromeda::andromeda()
  andromeda$firstTable <- data.frame(
    column1 = 1:5,
    column2 = rep('empty', 5)
  )

  andromeda$secondTable <- data.frame(
    column3 = 1:5,
    column4 = rep('empty', 5)
  )

  # checking it works with empty
  andromeda$emptyTable <- data.frame(
    column5 = 1,
    column6 = 'fake'
  )[-1,]

  andromeda <-addDbAndSettings(
    andromeda = andromeda,
    databaseId = 1,
    settingId = 'madeup'
  )

  testthat::expect_true(sum(names(andromeda) %in% c('firstTable','secondTable', 'emptyTable')) == 3)
  testthat::expect_true(sum(colnames(andromeda$firstTable) %in%
                              c("column1", "column2", "databaseId", "settingId")) == 4)
  testthat::expect_true(sum(colnames(andromeda$secondTable) %in%
                              c("column3", "column4", "databaseId", "settingId")) == 4)
  testthat::expect_true(sum(colnames(andromeda$emptyTable) %in%
                              c("column5", "column6", "databaseId", "settingId")) == 4)

  testthat::expect_true(unique(as.data.frame(andromeda$firstTable)$databaseId) == 1)
  testthat::expect_true(unique(as.data.frame(andromeda$firstTable)$settingId) == 'madeup')
  testthat::expect_true(unique(as.data.frame(andromeda$secondTable)$databaseId) == 1)
  testthat::expect_true(unique(as.data.frame(andromeda$secondTable)$settingId) == 'madeup')

  testthat::expect_true(nrow(as.data.frame(andromeda$emptyTable)) == 0)

})

test_that("saveCharacterizationAndromeda", {

  andromeda <- Andromeda::andromeda()
  andromeda$firstTable <- data.frame(
    column1 = 1:5,
    column2 = rep('empty', 5)
  )

  saveCharacterizationAndromeda(
    andromeda = andromeda,
    outputFolder = tempFolder
  )

  testthat::expect_true(file.exists(file.path(tempFolder, 'result')))

  andromeda <- Andromeda::loadAndromeda(file.path(tempFolder, 'result'))

  testthat::expect_true(names(andromeda) == 'firstTable')

  testthat::expect_identical(
    as.data.frame(andromeda$firstTable),
    data.frame(
      column1 = 1:5,
      column2 = rep('empty', 5)
    )

  )

  file.remove(file.path(tempFolder, 'result'))

})


test_that("exportAndromedaSubfilesToCsv", {

  # inside tempFolder create folders with Andromeda result file

  dir.create(file.path(tempFolder, 'test_1'))
  on.exit(unlink(file.path(tempFolder, 'test_1'), recursive = TRUE), add = TRUE)
  dir.create(file.path(tempFolder, 'test_2'))
  on.exit(unlink(file.path(tempFolder, 'test_1'), recursive = TRUE), add = TRUE)

  andromeda1 <- Andromeda::andromeda()
  andromeda1$analysisRef <- data.frame(
    analysisId = 1:3,
    databaseId = 1,
    settingId = rep('hello', 3)
  )
  andromeda1$covariateRef <- data.frame(
    covariateId = 1:5,
    databaseId = 1,
    settingId = rep('none', 5)
  )
  andromeda1$targetCovariates <- data.frame(
    covariateId = 1:5,
    databaseId = 1,
    settingId = rep('none', 5),
    sumValue = c(4,20,100,2,25),
    averageValue = c(4,20,100,2,25)/200
  )

  andromeda2 <- Andromeda::andromeda()
  andromeda2$analysisRef <- data.frame(
    analysisId = 1,
    databaseId = 1,
    settingId = rep('hi', 1)
  )
  andromeda2$covariateRef <- data.frame(
    covariateId = 1,
    databaseId = 1,
    settingId = rep('none', 1)
  )

  Andromeda::saveAndromeda(
    andromeda = andromeda1,
    fileName = file.path(tempFolder, 'test_1', 'result')
      )
  Andromeda::saveAndromeda(
    andromeda = andromeda2,
    fileName = file.path(tempFolder, 'test_2', 'result')
  )

  # 1) General test

  exportAndromedaSubfilesToCsv(
    executionPath = tempFolder,
    outputFolder = tempFolder2,
    csvFilePrefix = '',
    batchSize = 100000,
    minCellCount = 0,
    tablesToExport = c("analysisRef", "covariateRef")
  )

  # make sure two csv files exist
  testthat::expect_true(sum(c("analysis_ref.csv", "covariate_ref.csv") %in% dir(tempFolder2)) == 2)

  # make sure nrows are correct
  analysisRef <- read.csv(file.path(tempFolder2, "analysis_ref.csv"))
  testthat::expect_true(nrow(analysisRef) == 4)

  covRef <- read.csv(file.path(tempFolder2, "covariate_ref.csv"))
  testthat::expect_true(nrow(covRef) == 5) # Not correct

  # check tracker
  #readRDS(file.path(tempFolder2, 'tracker.rds'))

  # make sure all columns are there
  testthat::expect_true(sum(colnames(analysisRef) %in% c('analysis_id', 'database_id', 'setting_id')) == 3)
  testthat::expect_true(sum(colnames(covRef) %in% c('covariate_id', 'database_id', 'setting_id')) == 3)


  # 2) Testing csv prefix
  exportAndromedaSubfilesToCsv(
    executionPath = tempFolder,
    outputFolder = tempFolder2,
    csvFilePrefix = 'c_',
    batchSize = 100000,
    minCellCount = 0,
    tablesToExport = c("analysisRef", "covariateRef")
  )

  testthat::expect_true(sum(c("c_analysis_ref.csv", "c_covariate_ref.csv") %in% dir(tempFolder2)) == 2)



  # 3) Testing batchSize
  exportAndromedaSubfilesToCsv(
    executionPath = tempFolder,
    outputFolder = tempFolder2,
    csvFilePrefix = 'c_',
    batchSize = 1,
    minCellCount = 0,
    tablesToExport = c("analysisRef", "covariateRef")
  )
  # make sure two csv files exist
  testthat::expect_true(sum(c("c_analysis_ref.csv", "c_covariate_ref.csv") %in% dir(tempFolder2)) == 2)

  # make sure nrows are correct
  analysisRef <- read.csv(file.path(tempFolder2, "c_analysis_ref.csv"))
  testthat::expect_true(nrow(analysisRef) == 4)

  covRef <- read.csv(file.path(tempFolder2, "c_covariate_ref.csv"))
  testthat::expect_true(nrow(covRef) == 5) # Not correct

  # check tracker
  #readRDS(file.path(tempFolder2, 'tracker.rds'))

  # make sure all columns are there
  testthat::expect_true(sum(colnames(analysisRef) %in% c('analysis_id', 'database_id', 'setting_id')) == 3)
  testthat::expect_true(sum(colnames(covRef) %in% c('covariate_id', 'database_id', 'setting_id')) == 3)


  # 4) Testing minCellCount - will test more extensively below in censorResults test
  exportAndromedaSubfilesToCsv(
    executionPath = tempFolder,
    outputFolder = tempFolder2,
    csvFilePrefix = 'c_',
    batchSize = 10,
    minCellCount = 10000,
    tablesToExport = c("analysisRef", "covariateRef", "targetCovariates")
  )

  testthat::expect_true(sum(c("c_analysis_ref.csv", "c_covariate_ref.csv", "c_target_covariates.csv") %in% dir(tempFolder2)) == 3)

  targetCovs <- read.csv(file.path(tempFolder2, "c_target_covariates.csv"))
  testthat::expect_true(nrow(targetCovs) == 5)

  testthat::expect_true(unique(targetCovs$sum_value) == -10000)
  testthat::expect_true(is.na(unique(targetCovs$average_value)))

})


test_that("removeRedundant", {
  dir.create(file.path(tempFolder, 'test_3'))
  on.exit(unlink(file.path(tempFolder, 'test_3'), recursive = TRUE), add = TRUE)

  andromeda1 <- Andromeda::andromeda()
  andromeda1$analysisRef <- data.frame(
    analysisId = 1:100,
    databaseId = 1,
    settingId = rep('hello', 100)
  )
  andromeda1$covariateRef <- data.frame(
    covariateId = 1:20,
    databaseId = 1,
    settingId = rep('none', 20)
  )


  anIds <- as.data.frame(
    andromeda1$analysisRef
  ) %>%
    dplyr::filter(.data$analysisId %in% c(1,99)) %>%
    dplyr::mutate(
      uniqueId = paste0(.data$settingId, "-", as.character(format(as.double(.data$analysisId), nsmall = 0,  scientific = FALSE, trim = TRUE )))
    ) %>%
    dplyr::select("uniqueId") %>%
    dplyr::pull()

  covIds <- as.data.frame(
    andromeda1$covariateRef
  ) %>%
    dplyr::filter(.data$covariateId %in% c(5,6,18)) %>%
    dplyr::mutate(
      uniqueId = paste0(.data$settingId, "-", as.character(format(as.double(.data$covariateId), nsmall = 0,  scientific = FALSE, trim = TRUE )))
    ) %>%
    dplyr::select("uniqueId") %>%
    dplyr::pull()

  tracker <- list(
    covariateRefTracker = covIds,
    analysisRefTracker = anIds
  )

  saveRDS(tracker, file.path(tempFolder2, 'tracker.rds'))


  # 1) covariateRef

  covref <- removeRedundant(
    andromeda = andromeda1,
    tableName = 'covariateRef',
    csvTrackerFile = file.path(tempFolder2, 'tracker.rds')
  )

  # 17 new ids should be added
  updatedTracker <- readRDS(file.path(tempFolder2, 'tracker.rds'))
  testthat::expect_true(length(updatedTracker$covariateRefTracker) == 20)
  # covariateIds c(5,6,18) should be gone
  testthat::expect_true(nrow(as.data.frame(covref)) == 17)
  testthat::expect_true(sum(as.data.frame(covref)$covariateId %in% c(5,6,18)) == 0)



  # 1) analysisRef

  anref <- removeRedundant(
    andromeda = andromeda1,
    tableName = 'analysisRef',
    csvTrackerFile = file.path(tempFolder2, 'tracker.rds')
  )

  # 17 new ids should be added
  updatedTracker <- readRDS(file.path(tempFolder2, 'tracker.rds'))
  testthat::expect_true(length(updatedTracker$analysisRefTracker) == 100)
  # analysisIds c(1,99) should be gone
  testthat::expect_true(nrow(as.data.frame(anref)) == 98)
  testthat::expect_true(sum(as.data.frame(anref)$analysisId %in% c(1,99)) == 0)


})

test_that("censorResults", {

  # test targetCovariates
  data <- data.frame(
    covariateId = 1:5,
    databaseId = '1',
    settingId = 'madeup',
    sumValue = c(4,1,11,14,150),
    averageValue = c(4,1,11,14,150)/200
  )

  newdata <- censorResults(
    data = data,
    tableName = 'targetCovariates',
    minCellCount = 0
  )

  # check minCellCount 0 does nothing
  testthat::expect_identical(data, newdata)

  newdata <- censorResults(
    data = data,
    tableName = 'targetCovariates',
    minCellCount = 10
  )
   censored <- data$sumValue < 10
   testthat::expect_true(unique(newdata$sumValue[censored]) == -10)
   testthat::expect_true(is.na(unique(newdata$averageValue[censored])))

   notcensored <- data$sumValue >= 10
   testthat::expect_identical(newdata$sumValue[notcensored], data$sumValue[notcensored])

  # test riskFactorCovariates
  data <- data.frame(
    covariateId = 1:5,
    databaseId = '1',
    settingId = 'madeup',
    caseSumValue = c(4,1,11,14,150),
    caseAverageValue = c(4,1,11,14,150)/200,
    nonCaseSumValue = c(1,0,100,90,50),
    nonCaseAverageValue = c(1,0,100,90,50)/100
  )

  newdata <- censorResults(
    data = data,
    tableName = 'riskFactorCovariates',
    minCellCount = 10
  )

  censored <- data$caseSumValue < 10 & data$caseSumValue !=0
  testthat::expect_true(unique(newdata$caseSumValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$caseAverageValue[censored])))
  notcensored <- data$caseSumValue >= 10 | data$caseSumValue == 0
  testthat::expect_identical(newdata$caseSumValue[notcensored], data$caseSumValue[notcensored])

  censored <- data$nonCaseSumValue < 10 & data$nonCaseSumValue !=0
  testthat::expect_true(unique(newdata$nonCaseSumValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$nonCaseAverageValue[censored])))
  notcensored <- data$nonCaseSumValue >= 10 | data$nonCaseSumValue == 0
  testthat::expect_identical(newdata$nonCaseSumValue[notcensored], data$nonCaseSumValue[notcensored])


  # test caseSeriesCovariates
  data <- data.frame(
    covariateId = 1:5,
    databaseId = '1',
    settingId = 'madeup',
    beforeSumValue = c(4,1,11,14,150),
    beforeAverageValue = c(4,1,11,14,150)/200,
    duringSumValue = c(1,0,100,90,50),
    duringAverageValue = c(1,0,100,90,50)/200,
    afterSumValue = c(1,0,200,9,50),
    afterAverageValue = c(1,0,200,9,50)/200
  )

  newdata <- censorResults(
    data = data,
    tableName = 'caseSeriesCovariates',
    minCellCount = 10
  )

  censored <- data$beforeSumValue < 10 & data$beforeSumValue !=0
  testthat::expect_true(unique(newdata$beforeSumValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$beforeAverageValue[censored])))
  notcensored <- data$beforeSumValue >= 10 | data$beforeSumValue == 0
  testthat::expect_identical(newdata$beforeSumValue[notcensored], data$beforeSumValue[notcensored])

  censored <- data$duringSumValue < 10 & data$duringSumValue !=0
  testthat::expect_true(unique(newdata$duringSumValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$duringAverageValue[censored])))
  notcensored <- data$duringSumValue >= 10 | data$duringSumValue == 0
  testthat::expect_identical(newdata$duringSumValue[notcensored], data$duringSumValue[notcensored])

  censored <- data$afterSumValue < 10 & data$afterSumValue !=0
  testthat::expect_true(unique(newdata$afterSumValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$afterAverageValue[censored])))
  notcensored <- data$afterumValue >= 10 | data$afterSumValue == 0
  testthat::expect_identical(newdata$afterSumValue[notcensored], data$afterSumValue[notcensored])


  # CONTINIOUS COVARIATES


  # Time to event
  data <- data.frame(
    databaseId = '1',
    targetCohortDefinitionId = 1,
    outcomeCohortDefinitionId = 3,
    outcomeType = 'first',
    targetOutcomeType = 'after last',
    timeToEvent = c(1,2,3,4,5,10),
    numEvents = c(10,8,12,100,0,5),
    timeScale = 'per 1-day'
  )

  newdata <- censorResults(
    data = data,
    tableName = 'timeToEvent',
    minCellCount = 10
  )
  censored <- data$numEvents < 10 & data$numEvents !=0
  testthat::expect_true(unique(newdata$numEvents[censored]) == -10)
  notcensored <- data$numEvents  >= 10 | data$numEvents == 0
  testthat::expect_identical(newdata$numEvents[notcensored], data$numEvents[notcensored])



  # Dechallenge- rechall
  data <- data.frame(
    databaseId = '1',
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30,
    targetCohortDefinitionId = 1,
    outcomeCohortDefinitionId = 3,
    numExposureEras = c(100,10,3,5),
    numPersonsExposed = c(50,10,3,5),
    numCases = c(12,5,1,0),
    dechallengeAttempt = c(12,5,1,0),
    dechallengeFail = c(6,5,1,0),
    dechallengeSuccess = c(6,0,0,0),
    rechallengeAttempt = c(12,5,1,0),
    rechallengeFail = c(7,2,1,0),
    rechallengeSuccess = c(5,3,0,0),
    pctDechallengeAttempt = 0.2,
    pctDechallengeSuccess = 0.2,
    pctDechallengeFail = 0.2,
    pctRechallengeAttempt = 0.2,
    pctRechallengeSuccess = 0.2,
    pctRechallengeFail = 0.2
  )
  newdata <- censorResults(
    data = data,
    tableName = 'dechallengeRechallenge',
    minCellCount = 5
  )

  censored <- data$numExposureEras < 5 & data$numExposureEras !=0
  testthat::expect_true(unique(newdata$numExposureEras[censored]) == -5)
  notcensored <- data$numExposureEras  >= 5 | data$numExposureEras == 0
  testthat::expect_identical(newdata$numExposureEras[notcensored], data$numExposureEras[notcensored])

  #numPersonsExposed
  censored <- data$numExposureEras < 5 & data$numPersonsExposed !=0
  testthat::expect_true(unique(newdata$numPersonsExposed[censored]) == -5)
  notcensored <- data$numPersonsExposed  >= 5 | data$numPersonsExposed == 0
  testthat::expect_identical(newdata$numPersonsExposed[notcensored], data$numPersonsExposed[notcensored])

  # numCases
  censored <- data$numCases < 5 & data$numCases !=0
  testthat::expect_true(unique(newdata$numCases[censored]) == -5)
  notcensored <- data$numCases  >= 5 | data$numCases == 0
  testthat::expect_identical(newdata$numCases[notcensored], data$numCases[notcensored])

  #dechallengeAttempt - links to pctDechallengeAttempt
  censored <- data$dechallengeAttempt < 5 & data$dechallengeAttempt !=0
  testthat::expect_true(unique(newdata$dechallengeAttempt[censored]) == -5)
  testthat::expect_true(is.na(unique(newdata$pctDechallengeAttempt[censored])))
  notcensored <- data$dechallengeAttempt  >= 5 | data$dechallengeAttempt == 0
  testthat::expect_identical(newdata$dechallengeAttempt[notcensored], data$dechallengeAttempt[notcensored])

  #dechallengeFail - links to dechallengeSuccess, pctDechallengeFail, pctDechallengeSuccess
  censored <- data$dechallengeFail < 5 & data$dechallengeFail !=0
  testthat::expect_true(unique(newdata$dechallengeFail[censored]) == -5)
  testthat::expect_true(unique(newdata$dechallengeSuccess[censored]) == -5)
  testthat::expect_true(is.na(unique(newdata$pctDechallengeFail[censored])))
  testthat::expect_true(is.na(unique(newdata$pctDechallengeSuccess[censored])))
  notcensored <- data$dechallengeFail  >= 5 | data$dechallengeFail == 0
  testthat::expect_identical(newdata$dechallengeFail[notcensored], data$dechallengeFail[notcensored])

  #rechallengeAttempt - links to pctRechallengeAttempt
  censored <- data$rechallengeAttempt < 5 & data$rechallengeAttempt !=0
  testthat::expect_true(unique(newdata$rechallengeAttempt[censored]) == -5)
  testthat::expect_true(is.na(unique(newdata$pctRechallengeAttempt[censored])))
  notcensored <- data$rechallengeAttempt  >= 5 | data$rechallengeAttempt == 0
  testthat::expect_identical(newdata$rechallengeAttempt[notcensored], data$rechallengeAttempt[notcensored])

  #rechallengeFail - links to rechallengeSuccess, pctRechallengeFail, pctRechallengeSuccess
  censored <- data$rechallengeFail < 5 & data$rechallengeFail !=0
  testthat::expect_true(unique(newdata$rechallengeFail[censored]) == -5)
  testthat::expect_true(unique(newdata$rechallengeSuccess[censored]) == -5)
  testthat::expect_true(is.na(unique(newdata$pctRechallengeFail[censored])))
  testthat::expect_true(is.na(unique(newdata$pctRechallengeSuccess[censored])))
  notcensored <- data$rechallengeFail  >= 5 | data$rechallengeFail == 0
  testthat::expect_identical(newdata$rechallengeFail[notcensored], data$rechallengeFail[notcensored])



  # CONTINUOUS COVS
  #caseSeriesCovariatesContinuous
  data <- data.frame(
    covariateId = 1:5,
    databaseId = '1',
    settingId = 'madeup',
    beforeCountValue = c(4,1,11,14,150),
    beforeAverageValue = c(4,1,11,14,150)/200,
    beforeMinValue = rep(1,5),
    beforeMaxValue = rep(1,5),
    beforeStandardDeviation = rep(1,5),
    beforeMedianValue = rep(1,5),
    beforeP10Value = rep(1,5),
    beforeP25Value = rep(1,5),
    beforeP75Value = rep(1,5),
    beforeP90Value =rep(1,5),

    duringCountValue = c(1,0,100,90,50),
    duringAverageValue = c(1,0,100,90,50)/200,
    duringMinValue = rep(1,5),
    duringMaxValue = rep(1,5),
    duringStandardDeviation = rep(1,5),
    duringMedianValue = rep(1,5),
    duringP10Value = rep(1,5),
    duringP25Value = rep(1,5),
    duringP75Value = rep(1,5),
    duringP90Value =rep(1,5),

    afterCountValue = c(1,0,200,9,50),
    afterAverageValue = c(1,0,200,9,50)/200,
    afterMinValue = rep(1,5),
    afterMaxValue = rep(1,5),
    afterStandardDeviation = rep(1,5),
    afterMedianValue = rep(1,5),
    afterP10Value = rep(1,5),
    afterP25Value = rep(1,5),
    afterP75Value = rep(1,5),
    afterP90Value =rep(1,5)
  )

  newdata <- censorResults(
    data = data,
    tableName = 'caseSeriesCovariatesContinuous',
    minCellCount = 10
  )

  censored <- data$beforeCountValue < 10 & data$beforeCountValue !=0
  testthat::expect_true(unique(newdata$beforeCountValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$beforeAverageValue[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeMinValue[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeMaxValue[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeMedianValue[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeP10Value[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeP25Value[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeP75Value[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeP90Value[censored])))
  testthat::expect_true(is.na(unique(newdata$beforeStandardDeviation[censored])))
  notcensored <- data$beforeCountValue >= 10 | data$beforeCountValue == 0
  testthat::expect_identical(newdata$beforeCountValue[notcensored], data$beforeCountValue[notcensored])

  censored <- data$duringCountValue < 10 & data$duringCountValue !=0
  testthat::expect_true(unique(newdata$duringCountValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$duringAverageValue[censored])))
  testthat::expect_true(is.na(unique(newdata$duringMinValue[censored])))
  testthat::expect_true(is.na(unique(newdata$duringMaxValue[censored])))
  testthat::expect_true(is.na(unique(newdata$duringMedianValue[censored])))
  testthat::expect_true(is.na(unique(newdata$duringP10Value[censored])))
  testthat::expect_true(is.na(unique(newdata$duringP25Value[censored])))
  testthat::expect_true(is.na(unique(newdata$duringP75Value[censored])))
  testthat::expect_true(is.na(unique(newdata$duringP90Value[censored])))
  testthat::expect_true(is.na(unique(newdata$duringStandardDeviation[censored])))
  notcensored <- data$duringCountValue >= 10 | data$duringCountValue == 0
  testthat::expect_identical(newdata$duringCountValue[notcensored], data$duringCountValue[notcensored])

  censored <- data$afterCountValue < 10 & data$afterCountValue !=0
  testthat::expect_true(unique(newdata$afterCountValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$afterAverageValue[censored])))
  testthat::expect_true(is.na(unique(newdata$afterMinValue[censored])))
  testthat::expect_true(is.na(unique(newdata$afterMaxValue[censored])))
  testthat::expect_true(is.na(unique(newdata$afterMedianValue[censored])))
  testthat::expect_true(is.na(unique(newdata$afterP10Value[censored])))
  testthat::expect_true(is.na(unique(newdata$afterP25Value[censored])))
  testthat::expect_true(is.na(unique(newdata$afterP75Value[censored])))
  testthat::expect_true(is.na(unique(newdata$afterP90Value[censored])))
  testthat::expect_true(is.na(unique(newdata$afterStandardDeviation[censored])))
  notcensored <- data$afterCountValue >= 10 | data$afterCountValue == 0
  testthat::expect_identical(newdata$afterCountValue[notcensored], data$afterCountValue[notcensored])


  # Risk Factor continuous
  data <- data.frame(
    covariateId = 1:5,
    databaseId = '1',
    settingId = 'madeup',

    caseCountValue = c(4,1,11,14,150),
    caseAverageValue = c(4,1,11,14,150)/200,
    caseMinValue = rep(1,5),
    caseMaxValue = rep(1,5),
    caseStandardDeviation = rep(1,5),
    caseMedianValue = rep(1,5),
    caseP10Value = rep(1,5),
    caseP25Value = rep(1,5),
    caseP75Value = rep(1,5),
    caseP90Value =rep(1,5),

    nonCaseCountValue = c(1,0,100,90,50),
    nonCaseAverageValue = c(1,0,100,90,50)/200,
    nonCaseMinValue = rep(1,5),
    nonCaseMaxValue = rep(1,5),
    nonCaseStandardDeviation = rep(1,5),
    nonCaseMedianValue = rep(1,5),
    nonCaseP10Value = rep(1,5),
    nonCaseP25Value = rep(1,5),
    nonCaseP75Value = rep(1,5),
    nonCaseP90Value =rep(1,5)
  )

  newdata <- censorResults(
    data = data,
    tableName = 'riskFactorCovariatesContinuous',
    minCellCount = 10
  )

  censored <- data$caseCountValue < 10 & data$caseCountValue !=0
  testthat::expect_true(unique(newdata$caseCountValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$caseAverageValue[censored])))
  testthat::expect_true(is.na(unique(newdata$caseMinValue[censored])))
  testthat::expect_true(is.na(unique(newdata$caseMaxValue[censored])))
  testthat::expect_true(is.na(unique(newdata$caseMedianValue[censored])))
  testthat::expect_true(is.na(unique(newdata$caseP10Value[censored])))
  testthat::expect_true(is.na(unique(newdata$caseP25Value[censored])))
  testthat::expect_true(is.na(unique(newdata$caseP75Value[censored])))
  testthat::expect_true(is.na(unique(newdata$caseP90Value[censored])))
  testthat::expect_true(is.na(unique(newdata$caseStandardDeviation[censored])))
  notcensored <- data$caseCountValue >= 10 | data$caseCountValue == 0
  testthat::expect_identical(newdata$caseCountValue[notcensored], data$caseCountValue[notcensored])

  censored <- data$nonCaseCountValue < 10 & data$nonCaseCountValue !=0
  testthat::expect_true(unique(newdata$nonCaseCountValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$nonCaseAverageValue[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseMinValue[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseMaxValue[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseMedianValue[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseP10Value[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseP25Value[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseP75Value[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseP90Value[censored])))
  testthat::expect_true(is.na(unique(newdata$nonCaseStandardDeviation[censored])))
  notcensored <- data$nonCaseCountValue >= 10 | data$nonCaseCountValue == 0
  testthat::expect_identical(newdata$nonCaseCountValue[notcensored], data$nonCaseCountValue[notcensored])


  # target continuous
  data <- data.frame(
    covariateId = 1:5,
    databaseId = '1',
    settingId = 'madeup',
    countValue = c(4,1,11,14,150),
    averageValue = c(4,1,11,14,150)/200,
    minValue = rep(1,5),
    maxValue = rep(1,5),
    standardDeviation = rep(1,5),
    medianValue = rep(1,5),
    p10Value = rep(1,5),
    p25Value = rep(1,5),
    p75Value = rep(1,5),
    p90Value =rep(1,5)
  )

  newdata <- censorResults(
    data = data,
    tableName = 'targetCovariatesContinuous',
    minCellCount = 10
  )

  censored <- data$countValue < 10 & data$countValue !=0
  testthat::expect_true(unique(newdata$countValue[censored]) == -10)
  testthat::expect_true(is.na(unique(newdata$averageValue[censored])))
  testthat::expect_true(is.na(unique(newdata$minValue[censored])))
  testthat::expect_true(is.na(unique(newdata$maxValue[censored])))
  testthat::expect_true(is.na(unique(newdata$medianValue[censored])))
  testthat::expect_true(is.na(unique(newdata$p10Value[censored])))
  testthat::expect_true(is.na(unique(newdata$p25Value[censored])))
  testthat::expect_true(is.na(unique(newdata$p75Value[censored])))
  testthat::expect_true(is.na(unique(newdata$p90Value[censored])))
  testthat::expect_true(is.na(unique(newdata$standardDeviation[censored])))
  notcensored <- data$countValue >= 10 | data$countValue == 0
  testthat::expect_identical(newdata$countValue[notcensored], data$countValue[notcensored])

})


test_that("exportAttrition", {

  # create example attrition
  andromeda <- Andromeda::andromeda()
  andromeda$attrition <- data.frame(
    cohortDefinitionId = c(10,20,30,40, 11,21,31,12,22,32),
    attrReason = c('Target first in 365 - 365 prior obs',
                   'Target first in 365 - 365 prior obs',
                   'Target first in 365 - 365 prior obs',
                   'Target first in 365 - 365 prior obs',
                   'Cases','Cases','Cases',
                   '3. Has outcome during TAR',
                   '3. Has outcome during TAR',
                   '3. Has outcome during TAR'
                   ),
    n = c(1000,50,400,350,
          50,10,60,
          50,10,60
          ),
    databaseId = 'db',
    settingId = 'set1'
  )

  # save to temp folder
  saveCharacterizationAndromeda(
    andromeda = andromeda,
    outputFolder = file.path(tempFolder3,'attrition')
      )

  # now create target and case settings
  target_settings <- data.frame(
    target_id	= c(1,2,3,4),
    limit_to_first_in_n_days	= rep(365, 4),
    min_prior_observation	= rep(365, 4),
    setting_id	= 'set1',
    characterization_target_id	= c(10,20,30,40),
    database_id = 'db'
  )

  case_settings <- data.frame(
    outcome_id	= rep(3,3),
    outcome_washout_days	= rep(90,3),
    risk_window_start	= rep(1,3),
    start_anchor = rep('cohort_start',3),
    risk_window_end	= rep(365,3),
    end_anchor = rep('cohort_start',3),
    risk_factor_settings = rep(TRUE,3),
    case_series_settings = rep(FALSE,3),
    characterization_case_id	= c(1,2,3),
    setting_id	= 'set1',
    characterization_target_id	= c(10,20,40),
    database_id = 'db'
  )

  utils::write.csv(
    x = target_settings,
    file = file.path(tempFolder3, 'c_target_settings.csv')
      )

  utils::write.csv(
    x = case_settings,
    file = file.path(tempFolder3, 'c_case_settings.csv')
  )

exportAttrition(
    executionPath = tempFolder3,
    outputFolder = tempFolder3,
    csvFilePrefix = 'c_',
    minCellCount = 0
)

# load attrition
testthat::expect_true(file.exists(file.path(tempFolder3, 'c_attrition.csv')))

attrition <- utils::read.csv(file.path(tempFolder3, 'c_attrition.csv'))
testthat::expect_true(nrow(attrition) == 16)
testthat::expect_true(sum(colnames(attrition) %in% c('cohort_definition_id', 'attr_reason', 'n', 'database_id', 'setting_id')) == 5)


# now test the minCellCount
exportAttrition(
  executionPath = tempFolder3,
  outputFolder = tempFolder3,
  csvFilePrefix = 'c_',
  minCellCount = 50
)
attrition <- utils::read.csv(file.path(tempFolder3, 'c_attrition.csv'))
testthat::expect_true(sum(attrition$n < 50 & attrition$n != -50) == 0)
testthat::expect_true(sum(colnames(attrition) %in% c('cohort_definition_id', 'attr_reason', 'n', 'database_id', 'setting_id')) == 5)

# now test csvFilePrefix
utils::write.csv(
  x = target_settings,
  file = file.path(tempFolder3, 'cccd_target_settings.csv')
)

utils::write.csv(
  x = case_settings,
  file = file.path(tempFolder3, 'cccd_case_settings.csv')
)
exportAttrition(
  executionPath = tempFolder3,
  outputFolder = tempFolder3,
  csvFilePrefix = 'cccd_',
  minCellCount = 12
)
attrition <- utils::read.csv(file.path(tempFolder3, 'cccd_attrition.csv'))
testthat::expect_true(sum(attrition$n < 12 & attrition$n != -12) == 0)
testthat::expect_true(sum(colnames(attrition) %in% c('cohort_definition_id', 'attr_reason', 'n', 'database_id', 'setting_id')) == 5)


# test when no case_settings
utils::write.csv(
  x = target_settings,
  file = file.path(tempFolder3, 'c2_target_settings.csv')
)
return <- exportAttrition(
  executionPath = tempFolder3,
  outputFolder = tempFolder3,
  csvFilePrefix = 'c2_',
  minCellCount = 12
)
attrition <- utils::read.csv(file.path(tempFolder3, 'c2_attrition.csv'))
testthat::expect_true(sum(attrition$n < 12 & attrition$n != -12) == 0)
testthat::expect_true(sum(colnames(attrition) %in% c('cohort_definition_id', 'attr_reason', 'n', 'database_id', 'setting_id')) == 5)
testthat::expect_true(nrow(attrition) == 4)

# test no target or case settings
return <- exportAttrition(
  executionPath = tempFolder3,
  outputFolder = tempFolder3,
  csvFilePrefix = 'c3_',
  minCellCount = 12
)
testthat::expect_false(return)
testthat::expect_true(!file.exists(file.path(tempFolder3, 'c3_attrition.csv')))

})
