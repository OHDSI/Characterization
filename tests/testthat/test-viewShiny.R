context("ViewShiny")

# create a folder with results for the shiny app
resultLocation <- file.path(tempdir(),paste0('d_', paste0(sample(100,3), collapse = '_'), sep = ''), "shinyResults")
if (!dir.exists(resultLocation)) {
  dir.create(resultLocation, recursive = T)
}

test_that("is_installed", {
  testthat::expect_equal(is_installed("FeatureExtraction"), T)
  testthat::expect_equal(is_installed("MadeUp4u834t3f"), F)
})

test_that("ensure_installed", {
  testthat::expect_equal(ensure_installed("FeatureExtraction"), NULL)
})

test_that("prepareCharacterizationShiny works", {
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
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsRace = T
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

  runCharacterizationAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationSettings = characterizationSettings,
    outputDirectory = file.path(resultLocation, 'result'),
    executionPath = file.path(resultLocation, 'execution'),
    csvFilePrefix = "c_",
    databaseId = "1",
    threads = 1,
    incremental = T,
    minCellCount = 0,
    minCharacterizationMean = 0.01
  )

  settings <- Characterization:::prepareCharacterizationShiny(
    resultFolder = file.path(resultLocation,'result'),
    cohortDefinitionSet = NULL,
    sqliteLocation = file.path(resultLocation, "sqliteCharacterization", "sqlite.sqlite")
  )

  testthat::expect_true(settings$schema == "main")
  testthat::expect_true(settings$tablePrefix == "c_")
  testthat::expect_true(settings$cohortTablePrefix == "cg_")
  testthat::expect_true(settings$databaseTable == "DATABASE_META_DATA")

  connectionDetailsTest <- do.call(
    what = DatabaseConnector::createConnectionDetails,
    args = list(
      dbms = "sqlite",
      server = file.path(resultLocation, "sqliteCharacterization", "sqlite.sqlite")
    )
  )
  conTest <- DatabaseConnector::connect(connectionDetailsTest)
  tables <- tolower(
    DatabaseConnector::getTableNames(
      connection = conTest,
      databaseSchema = "main"
    )
  )

  # make sure the extra tables are added
  testthat::expect_true("cg_cohort_definition" %in% tables)
  testthat::expect_true("database_meta_data" %in% tables)
})

test_that("shiny app works", {
  settings <- prepareCharacterizationShiny(
    resultFolder  = file.path(resultLocation,'result'),
    cohortDefinitionSet = NULL,
    sqliteLocation = file.path(resultLocation, "sqliteCharacterization", "sqlite.sqlite")
  )

  app <- viewChars(
    databaseSettings = settings,
    testApp = T
  )

  shiny::testServer(
    app = app,
    args = list(),
    expr = {
      testthat::expect_equal(runServer[["About"]], 0)
      session$setInputs(menu = "About")
      testthat::expect_equal(runServer[["About"]], 1)
    }
  )
})
