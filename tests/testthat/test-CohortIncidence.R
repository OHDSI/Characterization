context("CohortIncidence")

test_that("createCohortIncidenceSettings validates inputs", {
  studyPopulationSettings <- createStudyPopulationSettings(targetIds = 1)

  settings <- createCohortIncidenceSettings(
    studyPopulationSettings = studyPopulationSettings,
    outcomeIds = c(2, 3),
    outcomeWashoutDays = 30,
    byAge = TRUE,
    ageBreaks = c(18, 65),
    startDate = "2026-01-01",
    endDate = "2026-12-31"
  )

  testthat::expect_s3_class(settings, "cohortIncidenceSettings")
  testthat::expect_equal(settings$outcomeWashoutDays, 30)
  testthat::expect_equal(settings$startDate, "2026-01-01")
  testthat::expect_equal(settings$endDate, "2026-12-31")

  testthat::expect_error(
    createCohortIncidenceSettings(
      studyPopulationSettings = studyPopulationSettings,
      outcomeIds = c(2, 3),
      outcomeWashoutDays = c(10, 20, 30)
    )
  )
  testthat::expect_error(
    createCohortIncidenceSettings(
      studyPopulationSettings = studyPopulationSettings,
      outcomeIds = 2,
      byAge = TRUE
    )
  )
  testthat::expect_error(
    createCohortIncidenceSettings(
      studyPopulationSettings = studyPopulationSettings,
      outcomeIds = 2,
      startDate = "20260101"
    )
  )
  testthat::expect_error(
    createCohortIncidenceSettings(
      studyPopulationSettings = studyPopulationSettings,
      outcomeIds = 2,
      startDate = "2026-02-29"
    )
  )
  testthat::expect_error(
    createCohortIncidenceSettings(
      studyPopulationSettings = studyPopulationSettings,
      outcomeIds = 2,
      startDate = "2026-12-31",
      endDate = "2026-01-01"
    )
  )
})

test_that("createCharacterizationSettings accepts cohort-incidence settings", {
  cohortIncidenceSettings <- createCohortIncidenceSettings(
    studyPopulationSettings = createStudyPopulationSettings(targetIds = 1),
    outcomeIds = 2
  )

  settings <- createCharacterizationSettings(
    cohortIncidenceSettings = cohortIncidenceSettings
  )

  testthat::expect_s3_class(settings, "characterizationSettings")
  testthat::expect_length(settings$cohortIncidenceSettings, 1)
  testthat::expect_true(
    length(settings$cohortIncidenceSettings[[1]]$characterizationTargetIds) > 0
  )
})

test_that("createCohortIncidenceDesign uses CohortIncidence inputs", {
  settings <- createCharacterizationSettings(
    cohortIncidenceSettings = createCohortIncidenceSettings(
      studyPopulationSettings = createStudyPopulationSettings(targetIds = 1),
      outcomeIds = c(2, 3),
      outcomeWashoutDays = 30,
      riskWindowStart = 1,
      startAnchor = "cohort start",
      riskWindowEnd = 30,
      endAnchor = "cohort end",
      byAge = TRUE,
      ageBreaks = c(18, 65),
      startDate = "2020-01-01",
      endDate = "2020-12-31"
    )
  )$cohortIncidenceSettings[[1]]

  design <- createCohortIncidenceDesign(settings)

  testthat::expect_s3_class(design, "IncidenceDesign")
  testthat::expect_equal(design$timeAtRiskDefs[[1]]$startWith, "start")
  testthat::expect_equal(design$timeAtRiskDefs[[1]]$endWith, "end")
  testthat::expect_equal(design$outcomeDefs[[1]]$name, "cohort 2")
  testthat::expect_length(design$timeAtRiskDefs, 1)
  testthat::expect_length(design$analysisList, 1)
  testthat::expect_silent(design$asJSON())

  defaultSettings <- createCharacterizationSettings(
    cohortIncidenceSettings = createCohortIncidenceSettings(
      studyPopulationSettings = createStudyPopulationSettings(targetIds = 1),
      outcomeIds = 2
    )
  )$cohortIncidenceSettings[[1]]

  testthat::expect_silent(createCohortIncidenceDesign(defaultSettings)$asJSON())
})

test_that("exportCohortIncidence uses characterization target IDs", {
  outputFolder <- tempfile("cohort_incidence_export")
  dir.create(outputFolder)
  withr::defer(unlink(outputFolder, recursive = TRUE, force = TRUE))

  design <- createCohortIncidenceDesign(
    createCharacterizationSettings(
      cohortIncidenceSettings = createCohortIncidenceSettings(
        studyPopulationSettings = createStudyPopulationSettings(targetIds = 1),
        outcomeIds = 2
      )
    )$cohortIncidenceSettings[[1]]
  )

  exportCohortIncidence(
    executeResults = list(
      incidence_summary = data.frame(target_cohort_definition_id = 1),
      target_def = data.frame(target_cohort_definition_id = 1)
    ),
    databaseId = "test_database",
    exportFolder = outputFolder,
    irDesign = design,
    refId = 1
  )

  result <- Andromeda::loadAndromeda(file.path(outputFolder, "result"))

  testthat::expect_true("characterizationTargetId" %in% names(result$incidenceSummary))
  testthat::expect_true("characterizationTargetId" %in% names(result$targetDef))
  testthat::expect_true("characterizationTargetId" %in% names(result$targetOutcomeRef))
})

test_that("runCharacterizationAnalyses executes cohort incidence", {
  skipIfCreateTargetCohortSqlUnavailable()

  outputFolder <- tempfile("cohort_incidence")
  withr::defer(
    unlink(outputFolder, recursive = TRUE, force = TRUE),
    testthat::teardown_env()
  )

  characterizationSettings <- createCharacterizationSettings(
    cohortIncidenceSettings = createCohortIncidenceSettings(
      studyPopulationSettings = createStudyPopulationSettings(targetIds = 1),
      outcomeIds = 2,
      riskWindowEnd = 30
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
    outputDatabaseSchema = "main",
    outputTable = "cohort_incidence_test",
    tempEmulationSchema = "main",
    outputDirectory = file.path(outputFolder, "result"),
    executionPath = file.path(outputFolder, "execution"),
    csvFilePrefix = "c_",
    incremental = FALSE,
    threads = 1,
    nTargetJobs = 1,
    mode = "CohortIncidence"
  )

  testthat::expect_true(
    file.exists(file.path(outputFolder, "execution", "ci_1", "result"))
  )
})