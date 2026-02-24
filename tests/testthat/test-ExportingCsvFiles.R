context("ExportingCsvFiles")
library(dplyr)

tempFolder <- file.path(tempdir(),"exporting")
on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

tempFolder2 <- file.path(tempdir(),"results")
on.exit(unlink(tempFolder2, recursive = TRUE), add = TRUE)

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

  newdata <- Characterization:::censorResults(
    data = data,
    tableName = 'targetCovariates',
    minCellCount = 0
  )

  # check minCellCount 0 does nothing
  testthat::expect_identical(data, newdata)

  newdata <- Characterization:::censorResults(
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

  newdata <- Characterization:::censorResults(
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

  newdata <- Characterization:::censorResults(
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

  newdata <- Characterization:::censorResults(
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
  newdata <- Characterization:::censorResults(
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

})
