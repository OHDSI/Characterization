# library(Characterization)
# library(testthat)

context("DechallengeRechallenge")

tempDbLoc <- tempfile(fileext = ".sqlite")
on.exit(unlink(tempDbLoc))
connectionDetailsReal <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = tempDbLoc
)

test_that("createDechallengeRechallengeSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))

  res <- createDechallengeRechallengeSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 0,
      minPriorObservation = 0
    ),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  testthat::expect_true(
    inherits(res, "dechallengeRechallengeSettings")
  )

  testthat::expect_equal(
    res$studyPopulationSettings$targetId,
    targetIds
  )

  testthat::expect_equal(
    res$outcomeCohortDefinitionIds,
    outcomeIds
  )

  testthat::expect_equal(
    res$dechallengeStopInterval,
    30
  )

  testthat::expect_equal(
    res$dechallengeEvaluationWindow,
    31
  )
})

test_that("computeDechallengeRechallengeAnalyses", {
  targetIds <- c(2)
  outcomeIds <- c(3, 4)

  drSet <- createDechallengeRechallengeSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 0,
      minPriorObservation = 0
    ),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )

  charSet <- createCharacterizationSettings(
    dechallengeRechallengeSettings = drSet
    )

  charSet$dechallengeRechallengeSettings[[1]]$characterizationTargetIds <- 2

  # make the cohorts in a table
  dcLoc <- tempfile("runADechal")
  dc <- Characterization::computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetails,
    #targetDatabaseSchema = "main",
    #targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationDatabaseSchema = "main",
    characterizationTable = "cohort",
    settings = charSet$dechallengeRechallengeSettings[[1]],
    databaseId = "testing",
    outputFolder = dcLoc
  )
  testthat::expect_true(dc)

  # No results with Andromeda - so also try made up data
  # check with made up date
  # subject 1 has 1 exposure for 30 days
  # subject 2 has 4 exposures for ~30 days with ~30 day gaps
  # subject 3 has 3 exposures for ~30 days with ~30 day gaps
  # subject 4 has 2 exposures for ~30 days with ~30 day gaps
  targetCohort <- data.frame(
    cohort_definition_id = rep(1, 10),
    subject_id = c(1, 2, 2, 2, 2, 3, 3, 3, 4, 4),
    cohort_start_date = as.Date(c(
      "2001-01-01",
      "2001-01-01", "2001-03-14", "2001-05-01", "2001-07-01",
      "2001-01-01", "2001-03-01", "2001-05-01",
      "2001-01-01", "2001-03-01"
    )),
    cohort_end_date = as.Date(c(
      "2001-01-31",
      "2001-01-31", "2001-03-16", "2001-05-30", "2001-07-31",
      "2001-01-31", "2001-03-30", "2001-05-30",
      "2001-01-31", "2001-03-30"
    ))
  )

  # person 2 has it during 1st exposure and stops when 1st stops then restarts when 2nd starts and stops when 2nd stops
  # person 3 has it during 2nd exposure and stops when 2nd stops
  # person 4 has outcome whole time after 2nd exposure

  outcomeCohort <- data.frame(
    cohort_definition_id = rep(2, 4),
    subject_id = c(2, 2, 3, 4),
    cohort_start_date = as.Date(c(
      "2001-01-28", "2001-03-15",
      "2001-03-01",
      "2001-03-05"
    )),
    cohort_end_date = as.Date(c(
      "2001-02-03", "2001-03-16",
      "2001-03-30",
      "2010-03-05"
    ))
  )

  con <- DatabaseConnector::connect(connectionDetails = connectionDetailsReal)

  DatabaseConnector::insertTable(
    data = rbind(targetCohort, outcomeCohort),
    connection = con,
    databaseSchema = "main",
    tableName = "cohort_dechal",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    camelCaseToSnakeCase = FALSE
  )

  DatabaseConnector::disconnect(con)

  drSet <- createDechallengeRechallengeSettings(
    targetIds = 1,
    outcomeIds = 2,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )

  charSet <- createCharacterizationSettings(
    dechallengeRechallengeSettings = drSet
  )

  charSet$dechallengeRechallengeSettings[[1]]$characterizationTargetIds <- 1


  dcLoc <- tempfile("runADechal2")
  dc <- computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetailsReal,
    #targetDatabaseSchema = "main",
    #targetTable = "cohort_dechal",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort_dechal",
    characterizationDatabaseSchema = "main",
    characterizationTable = "cohort_dechal",
    settings = charSet$dechallengeRechallengeSettings[[1]],
    databaseId = "testing",
    outputFolder = dcLoc
  )

  res <- Andromeda::loadAndromeda(file.path(dcLoc, "result"))

  dc <- as.data.frame(res$dechallengeRechallenge)
  # one T and 2 Os, so should have 2 rows
  testthat::expect_true(nrow(dc) == 1)
  testthat::expect_true(dc$numPersonsExposed == 4)
  testthat::expect_true(dc$numExposureEras == 10)

  # clean up
  file.remove(file.path(dcLoc,"result"))
})

test_that("computeRechallengeFailCaseSeriesAnalyses with known data", {
  # check with made up date
  # subject 1 has 1 exposure for 30 days
  # subject 2 has 4 exposures for ~30 days with ~30 day gaps
  # subject 3 has 3 exposures for ~30 days with ~30 day gaps
  # subject 4 has 2 exposures for ~30 days with ~30 day gaps
  targetCohort <- data.frame(
    cohort_definition_id = rep(1, 10),
    subject_id = c(1, 2, 2, 2, 2, 3, 3, 3, 4, 4),
    cohort_start_date = as.Date(c(
      "2001-01-01",
      "2001-01-01", "2001-03-14", "2001-05-01", "2001-07-01",
      "2001-01-01", "2001-03-01", "2001-05-01",
      "2001-01-01", "2001-03-01"
    )),
    cohort_end_date = as.Date(c(
      "2001-01-31",
      "2001-01-31", "2001-03-16", "2001-05-30", "2001-07-31",
      "2001-01-31", "2001-03-30", "2001-05-30",
      "2001-01-31", "2001-03-30"
    ))
  )

  # person 2 has it during 1st exposure and stops when 1st stops then restarts when 2nd starts and stops when 2nd stops
  # person 3 has it during 2nd exposure and stops when 2nd stops
  # person 4 has outcome whole time after 2nd exposure

  outcomeCohort <- data.frame(
    cohort_definition_id = rep(2, 4),
    subject_id = c(2, 2, 3, 4),
    cohort_start_date = as.Date(c(
      "2001-01-28", "2001-03-15",
      "2001-03-01",
      "2001-03-05"
    )),
    cohort_end_date = as.Date(c(
      "2001-02-03", "2001-03-16",
      "2001-03-30",
      "2010-03-05"
    ))
  )

  con <- DatabaseConnector::connect(connectionDetails = connectionDetailsReal)

  DatabaseConnector::insertTable(
    data = rbind(targetCohort, outcomeCohort),
    connection = con,
    databaseSchema = "main",
    tableName = "cohort",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    camelCaseToSnakeCase = FALSE
  )
  DatabaseConnector::disconnect(con)

  drSet <- createDechallengeRechallengeSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = 1
    ),
    outcomeIds = 2,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30 # 31
  )
  charSet <- createCharacterizationSettings(
    dechallengeRechallengeSettings = drSet
  )

  # add the target_settings table
  con <- DatabaseConnector::connect(connectionDetails = connectionDetailsReal)

  DatabaseConnector::insertTable(
    data = data.frame(
      characterizationTargetIds = c(10),
      targetId = c(1)
    ),
    connection = con,
    databaseSchema = "main",
    tableName = "target_settings",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    camelCaseToSnakeCase = FALSE
  )
  DatabaseConnector::disconnect(con)

  # add the characterization cohort table "characterization"
  con <- DatabaseConnector::connect(connectionDetails = connectionDetailsReal)

  DatabaseConnector::insertTable(
    data = data.frame(
      cohort_definition_id = rep(10, 10),
      subject_id = c(1, 2, 2, 2, 2, 3, 3, 3, 4, 4),
      cohort_start_date = as.Date(c(
        "2001-01-01",
        "2001-01-01", "2001-03-14", "2001-05-01", "2001-07-01",
        "2001-01-01", "2001-03-01", "2001-05-01",
        "2001-01-01", "2001-03-01"
      )),
      cohort_end_date = as.Date(c(
        "2001-01-31",
        "2001-01-31", "2001-03-16", "2001-05-30", "2001-07-31",
        "2001-01-31", "2001-03-30", "2001-05-30",
        "2001-01-31", "2001-03-30"
      ))
    ),
    connection = con,
    databaseSchema = "main",
    tableName = "characterization",
    createTable = TRUE,
    dropTableIfExists = TRUE,
    camelCaseToSnakeCase = FALSE
  )
  DatabaseConnector::disconnect(con)

  dcLoc <- tempfile("runADechal2")
  dc <- computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetailsReal,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = charSet$dechallengeRechallengeSettings[[1]],
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationDatabaseSchema = "main",
    characterizationTable = "characterization",
    databaseId = "testing",
    outputFolder = dcLoc
  )

  res <- Andromeda::loadAndromeda(file.path(dcLoc, "result"))

  # person 2 should be in results
  dc <- as.data.frame(res$rechallengeFailCaseSeries)
  testthat::expect_equal(nrow(dc), 1)

  testthat::expect_true(is.na(dc$subjectId))

  dcLoc <- tempfile("runADechal3")
  dc <- Characterization::computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetailsReal,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    characterizationDatabaseSchema = "main",
    characterizationTable = "characterization",
    targetSettingsTable = "target_settings", # new
    settings = charSet$dechallengeRechallengeSettings[[1]],
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    databaseId = "testing",
    showSubjectId = TRUE,
    outputFolder = dcLoc
  )

  # person 2 should be in results
  res <- Andromeda::loadAndromeda(file.path(dcLoc, "result"))
  dc <- as.data.frame(res$rechallengeFailCaseSeries)

  testthat::expect_equal(nrow(dc), 1)
  testthat::expect_equal(dc$subjectId, 2)

  # clean up
  file.remove(file.path(dcLoc, "result"))

})


# add test for job creation code
test_that("getDechallengeRechallengeJobs", {
  targetIds <- c(2, 5, 6, 7, 8)
  outcomeIds <- c(3, 4, 9, 10)

  res <- createDechallengeRechallengeSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )
  charSettings <- createCharacterizationSettings(
    dechallengeRechallengeSettings = res
  )

  jobs <- getDechallengeRechallengeJobs(
    characterizationSettings = charSettings,
    nTargetJobs = 1
  )

  # as 1 thread should be 2 rows for two analyses
  testthat::expect_true(nrow(jobs) == 2)

  # check all target ids are in there
  targetIdFromSettings <- do.call(
    what = unique,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$characterizationTargetIds
    })
  )

  originalTs <- charSettings$characterizationTargetLookup$targetId[
    charSettings$characterizationTargetLookup$characterizationTargetId %in% targetIdFromSettings
  ]

  testthat::expect_true(sum(targetIds %in% originalTs) ==
    length(targetIds))

  # check all outcome ids are in there
  outcomeIdFromSettings <- do.call(
    what = unique,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$outcomeIds
    })
  )
  testthat::expect_true(sum(outcomeIds %in% outcomeIdFromSettings) ==
    length(outcomeIds))


  # checking more threads 3
  jobs <- getDechallengeRechallengeJobs(
    characterizationSettings = charSettings,
    nTargetJobs = 3
  )

  # as 3 thread should be 2*3 rows for two analyses
  testthat::expect_true(nrow(jobs) == 2 * 3)

  # check all target ids are in there
  targetIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$characterizationTargetIds
    })
  )

  originalTs <- charSettings$characterizationTargetLookup$targetId[
    charSettings$characterizationTargetLookup$characterizationTargetId %in% targetIdFromSettings
  ]

  testthat::expect_true(sum(targetIds %in% originalTs) ==
    length(targetIds))

  # check all outcome ids are in there
  outcomeIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$outcomeIds
    })
  )
  testthat::expect_true(sum(outcomeIds %in% outcomeIdFromSettings) ==
    length(outcomeIds))



  # checking more threads than needed 20
  jobs <- getDechallengeRechallengeJobs(
    characterizationSettings = charSettings,
    nTargetJobs = 20
  )

  # as 3 thread should be 2*5 rows for two analyses
  testthat::expect_true(nrow(jobs) == 2 * 5)

  # check all target ids are in there
  targetIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$characterizationTargetIds
    })
  )
  originalTs <- charSettings$characterizationTargetLookup$targetId[
    charSettings$characterizationTargetLookup$characterizationTargetId %in% targetIdFromSettings
  ]
  testthat::expect_true(sum(targetIds %in% originalTs) ==
    length(targetIds))

  # check all outcome ids are in there
  outcomeIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$outcomeIds
    })
  )
  testthat::expect_true(sum(outcomeIds %in% outcomeIdFromSettings) ==
    length(outcomeIds))
})
