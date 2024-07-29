library(testthat)

testPlatform <- function(dbmsDetails) {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)

  timeToEventSettings1 <- createTimeToEventSettings(
    targetIds = 1,
    outcomeIds = c(3, 4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    targetIds = 2,
    outcomeIds = c(3, 4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  aggregateCovariateSettings1 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsRace = T
    )
  )

  aggregateCovariateSettings2 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useConditionOccurrenceLongTerm = T
    )
  )

  characterizationSettings <- createCharacterizationSettings(
    timeToEventSettings = list(
      timeToEventSettings1,
      timeToEventSettings2
    ),
    dechallengeRechallengeSettings = list(
      dechallengeRechallengeSettings
    ),
    aggregateCovariateSettings = list(
      aggregateCovariateSettings1,
      aggregateCovariateSettings2
    )
  )

  tempFolder <- tempfile(paste0("Characterization_", dbmsDetails$connectionDetails$dbms))
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

  runCharacterizationAnalyses(
    connectionDetails = dbmsDetails$connectionDetails,
    cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
    targetDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    targetTable = "cohort",
    outcomeDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
    outcomeTable = "cohort",
    characterizationSettings = characterizationSettings,
    saveDirectory = tempFolder,
    tablePrefix = "c_",
    databaseId = dbmsDetails$connectionDetails$dbms
  )

  testthat::expect_true(
    file.exists(file.path(tempFolder, "tracker.csv"))
  )
  tracker <- readr::read_csv(
    file = file.path(tempFolder, "tracker.csv"),
    show_col_types = FALSE
  )
  testthat::expect_equal(nrow(tracker), 6)

  # check the sqlite database here using export to csv

  connectionDetailsT <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = file.path(tempFolder, "sqliteCharacterization", "sqlite.sqlite")
  )

  exportDatabaseToCsv(
    connectionDetails = connectionDetailsT,
    resultSchema = "main",
    tablePrefix = "c_",
    saveDirectory = file.path(tempFolder, "csv")
  )

  testthat::expect_true(
    length(dir(file.path(tempFolder, "csv"))) > 0
  )

  # check cohort details is saved
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "cohort_details.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "settings.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "analysis_ref.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "covariate_ref.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "dechallenge_rechallenge.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "rechallenge_fail_case_series.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "csv", "time_to_event.csv"))
  )
}

# This file contains platform specific tests
test_that("platform specific test", {
  # Note that these tests are designed to be quick and just test the platform in a general way
  # Sqlite completes the bulk of the packages testing
  for (dbmsPlatform in dbmsPlatforms) {
    dbmsDetails <- getPlatformConnectionDetails(dbmsPlatform)
    if (is.null(dbmsDetails)) {
      print(paste("No platform details available for", dbmsPlatform))
    } else {
      print(paste("Testing", dbmsPlatform))
      testPlatform(dbmsDetails)
    }
  }
})
