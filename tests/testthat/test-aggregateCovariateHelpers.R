# library(Characterization)
# library(testthat)

context("AggregateCovariateHelpers")

test_that("hash seed works", {

  hash1 <- hash(
    x = "{a:'fake json'}",
    seed = 0
    )
  hash2 <- hash(
    x = "{a:'fake json'}",
    seed = 1
  )

  testthat::expect_false(hash1 == hash2)
  testthat::expect_true(inherits(x = hash1, 'character'))

})

test_that("extractCovariateList works", {

  covList <- list(
    list(
      covariateSettings = FeatureExtraction::createDefaultCovariateSettings()
    ),
    list(
      covariateSettings = FeatureExtraction::createDefaultCovariateSettings()
    ),
    list(
      covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsAge = T)
    )
  )

  result <- extractCovariateList(settings = covList)
  testthat::expect_true(length(result) == 2)
  testthat::expect_true(identical(result[[1]], FeatureExtraction::createDefaultCovariateSettings()))
  testthat::expect_true(identical(result[[2]], FeatureExtraction::createCovariateSettings(useDemographicsAge = T)))

})

test_that("extractCaseCovariateList works", {

  covList <- list(
    list(
      covariateSettings = createDuringCovariateSettings(
        useConditionOccurrenceDuring = T
        )
    ),
    list(
      covariateSettings = createDuringCovariateSettings(
        useConditionOccurrenceDuring = T
      )
    ),
    list(
      covariateSettings = createDuringCovariateSettings(
        useConditionEraDuring = T
      )
    )
  )

  result <- Characterization:::extractCovariateList(settings = covList)
  testthat::expect_true(length(result) == 2)
  testthat::expect_true(identical(result[[1]], createDuringCovariateSettings(
    useConditionOccurrenceDuring = T
  )))
  testthat::expect_true(identical(result[[2]], createDuringCovariateSettings(
    useConditionEraDuring = T
  )))

})

test_that("extractCombinationSettings works", {

  covSet <- FeatureExtraction::createCovariateSettings(
    useDemographicsAge = T
  )
  targetIds <- c(1,2,4)
  riskWindowStart <- 1
  settings <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = 3,
    minPriorObservation = 10,
    outcomeWashoutDays = 100,
    riskWindowStart = riskWindowStart,
    startAnchor = rep("cohort start", length(riskWindowStart)),
    riskWindowEnd = rep(365,length(riskWindowStart)),
    endAnchor = rep("cohort start",length(riskWindowStart)),
    covariateSettings = covSet,
    caseCovariateSettings = createDuringCovariateSettings(
      useConditionEraDuring = T
    ),
    casePreTargetDuration = 300,
    casePostOutcomeDuration = 300
  )


  combo <- extractCombinationSettings(
    x = settings,
    covariateSettingsList = list(list(covSet)),
    caseCovariateSettingsList = list(createDuringCovariateSettings(
      useConditionEraDuring = T
    ))
  )

  testthat::expect_equal(
    nrow(combo),
    length(targetIds)*2+2+length(targetIds)*1*length(riskWindowStart)*4
      )


  targetIds <- c(1,2)
  riskWindowStart <- 1:5
  settings <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = 3,
    minPriorObservation = 10,
    outcomeWashoutDays = 100,
    riskWindowStart = riskWindowStart,
    startAnchor = rep("cohort start", length(riskWindowStart)),
    riskWindowEnd = rep(365,length(riskWindowStart)),
    endAnchor = rep("cohort start",length(riskWindowStart)),
    covariateSettings = covSet,
    caseCovariateSettings = createDuringCovariateSettings(
      useConditionEraDuring = T
    ),
    casePreTargetDuration = 300,
    casePostOutcomeDuration = 300
  )


  combo <- extractCombinationSettings(
    x = settings,
    covariateSettingsList = list(list(covSet)),
    caseCovariateSettingsList = list(createDuringCovariateSettings(
      useConditionEraDuring = T
    ))
  )

  testthat::expect_equal(
    nrow(combo),
    length(targetIds)*2+2+length(targetIds)*1*length(riskWindowStart)*4
  )


})

test_that("createFolderName works", {

names <- createFolderName(
    typeName = 'T',
    values = data.frame(
      a = 1:5,
      b = rep(1,5)
    )
)

testthat::expect_equal(
  names,
  c("T_1_1","T_2_1","T_3_1","T_4_1","T_5_1")
)

})

test_that("addFolderId works", {

  covSet <- FeatureExtraction::createCovariateSettings(
    useDemographicsAge = T
  )
  targetIds <- c(1,2,10,11)
  riskWindowStart <- 1:5
  settings <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = 3,
    minPriorObservation = 10,
    outcomeWashoutDays = 100,
    riskWindowStart = riskWindowStart,
    startAnchor = rep("cohort start", length(riskWindowStart)),
    riskWindowEnd = rep(365,length(riskWindowStart)),
    endAnchor = rep("cohort start",length(riskWindowStart)),
    covariateSettings = covSet,
    caseCovariateSettings = createDuringCovariateSettings(
      useConditionEraDuring = T
    ),
    casePreTargetDuration = 300,
    casePostOutcomeDuration = 300
  )


  cohortDetails <- Characterization:::extractCombinationSettings(
    x = settings,
    covariateSettingsList = list(list(covSet)),
    caseCovariateSettingsList = list(createDuringCovariateSettings(
      useConditionEraDuring = T
    ))
  )

  cohortDetailsWithFolder <- Characterization:::addFolderId(
    cohortDetails = cohortDetails,
    outputFolder = 'test',
    threads = 4
  )

  testthat::expect_true(max(cohortDetailsWithFolder$runId) <= 4)

  testthat::expect_true(nrow(cohortDetailsWithFolder) == nrow(cohortDetails))
  testthat::expect_true(ncol(cohortDetailsWithFolder) == (2+ncol(cohortDetails)))

  testthat::expect_true(
  sum(unique(cohortDetails$targetCohortId) %in%
    cohortDetailsWithFolder$targetCohortId) ==
    length(unique(cohortDetails$targetCohortId))
  )

  testthat::expect_true(
    sum(unique(cohortDetails$outcomeCohortId) %in%
          cohortDetailsWithFolder$outcomeCohortId) ==
      length(unique(cohortDetails$outcomeCohortId)
  ))

})

test_that("incremental files", {

 # removeExecuted(
 # cohortDetails,
 # executedDetails
 #)

# saveIncremental <- function(
#    cohortDetails,
#  incrementalFile
#)

  #loadIncremental <- function(
  #  incrementalFile
 #)

})

# extractTargetFeatures
# extractOutcomeFeatures
# extractCaseFeatures

#exportAndromedaToCsv <- function(
#    andromeda,
#    outputFolder,
#    cohortDetails,
#    counts,
#    databaseId,
#    minCharacterizationMean,
#    batchSize = 100000
#    )

#aggregateCsvs <- function(
#    outputFolder
#)

#saveSettings <- function(
#    outputFolder,
#    cohortDetails,
#    databaseId,
#    covariateSettingsList,
#    caseCovariateSettingsList
#)

