context("ViewShiny")

# create a folder with results for the shiny app
resultLocation <- file.path(tempdir(), paste0("d_", paste0(sample(100, 3), collapse = "_"), sep = ""), "shinyResults")
if (!dir.exists(resultLocation)) {
  dir.create(resultLocation, recursive = TRUE)
}

test_that("is_installed", {
  testthat::expect_equal(is_installed("FeatureExtraction"), TRUE)
  testthat::expect_equal(is_installed("MadeUp4u834t3f"), FALSE)
})

test_that("ensure_installed", {
  testthat::expect_equal(ensure_installed("FeatureExtraction"), NULL)
})

test_that("prepareCharacterizationShiny works", {
  skipIfCreateTargetCohortSqlUnavailable()

  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)

  studyPop1 <- createStudyPopulationSettings(targetIds = 1)
  studyPop2 <- createStudyPopulationSettings(targetIds = 2)
  studyPopAll <- createStudyPopulationSettings(targetIds = targetIds)

  timeToEventSettings1 <- createTimeToEventSettings(
    studyPopulationSettings = studyPop1,
    outcomeIds = c(3, 4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    studyPopulationSettings = studyPop2,
    outcomeIds = c(3, 4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    studyPopulationSettings = studyPopAll,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  targetSettings1 <- createTargetBaselineSettings(
    studyPopulationSettings = studyPopAll,
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE
    )
  )

  targetSettings2 <- createTargetBaselineSettings(
    studyPopulationSettings = studyPopAll,
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsAge = TRUE,
      useDemographicsRace = TRUE
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
    targetBaselineSettings = list(
      targetSettings1,
      targetSettings2
    )
  )

  runCharacterizationAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    nestingCohortDatabaseSchema = 'main',
    nestingCohortTable = "cohort",

    outputDatabaseSchema = 'main',
    outputTable = 'char_cohort',
    tempEmulationSchema = 'main',

    characterizationSettings = characterizationSettings,
    outputDirectory = file.path(resultLocation, "result"),
    executionPath = file.path(resultLocation, "execution"),
    csvFilePrefix = "c_",
    databaseId = "1",
    nTargetJobs = 1,
    threads = 1,
    incremental = TRUE,
    minCellCount = 0,
    minCharacterizationMean = 0.01,
    minSMD = 0,
    minCovariateCount = 0,
    mode = 'Efficient'
  )

  settings <- prepareCharacterizationShiny(
    resultFolder = file.path(resultLocation, "result"),
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
  on.exit(DatabaseConnector::disconnect(conTest))
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
