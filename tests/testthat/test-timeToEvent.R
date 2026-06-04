context("TimeToEvent")

test_that("createTimeToEventSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))

  res <- createTimeToEventSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds
    ),
    outcomeIds = outcomeIds
  )

  testthat::expect_true(
    length(unique(res$studyPopulationSettings$targetId)) == length(targetIds)
  )

  testthat::expect_true(
    length(unique(res$outcomeIds)) == length(outcomeIds)
  )
})

test_that("computeTimeToEventSettings", {
  skipIfCreateTargetCohortSqlUnavailable()

  targetIds <- c(1, 2)
  outcomeIds <- c(3, 4)

  res <- createTimeToEventSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds
    ),
    outcomeIds = outcomeIds
  )

  characterizationSettings <- createCharacterizationSettings(
    timeToEventSettings = res
  )

  jobDf <- getTimeToEventJobs(
    characterizationSettings = characterizationSettings,
    nTargetJobs = 1
  )

  tteFolder <- tempfile("tte")

  tables <- generateCohorts(
    characterizationSettings = characterizationSettings,
    mode = 'PatientLevelPrediction',
    incremental = FALSE,
    executionPath = tteFolder,
    connectionDetails = connectionDetails,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    outputDatabaseSchema = 'main',
    outputTable = 'char_cohort',
    cdmDatabaseSchema = "main",
    tempEmulationSchema = "main",
    progressBar = FALSE,
    settingHash = 'set1',
    dbHash = 'db1'
  )

  computeTimeToEventAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationDatabaseSchema = 'main',
    characterizationTable = tables$characterizationTable,
    settings = ParallelLogger::convertJsonToSettings(jobDf$settings[1]),
    outputFolder = tteFolder,
    databaseId = "tte_test"
  )

  testthat::expect_true(file.exists(file.path(tteFolder, "result")))

  res <- Andromeda::loadAndromeda(file.path(tteFolder, "result"))
  tte <- as.data.frame(res$timeToEvent)

  testthat::expect_true(nrow(tte) == 102)
  testthat::expect_true("databaseId" %in% colnames(tte))
  testthat::expect_true(tte$databaseId[1] == "tte_test")

  testthat::expect_true(
    length(
      unique(
        tte$targetCohortDefinition_id
      )
    ) <= length(targetIds)
  )

  charTargetIds <- characterizationSettings$characterizationTargetLookup$characterizationTargetId[
    characterizationSettings$characterizationTargetLookup$targetId %in% targetIds
  ]

  testthat::expect_true(
    sum(unique(
      tte$characterizationTargetId
    ) %in% charTargetIds) ==
      length(unique(tte$characterizationTargetId))
  )

  testthat::expect_true(
    length(
      unique(
        tte$outcomeCohortDefinitionId
      )
    ) <= length(outcomeIds)
  )
  testthat::expect_true(
    sum(
      unique(tte$outcomeCohortDefinitionId)
      %in% outcomeIds
    ) ==
      length(unique(tte$outcomeCohortDefinitionId))
  )

})
