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
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  testthat::expect_true(
    inherits(res, "dechallengeRechallengeSettings")
  )

  testthat::expect_equal(
    res$targetCohortDefinitionIds,
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

  res <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )

  dcLoc <- tempfile("runADechal")
  dc <- computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = res,
    databaseId = "testing",
    outputFolder = dcLoc,
    minCellCount = 0
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
    createTable = T,
    dropTableIfExists = T,
    camelCaseToSnakeCase = F
  )
  res <- createDechallengeRechallengeSettings(
    targetIds = 1,
    outcomeIds = 2,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )

  dcLoc <- tempfile("runADechal2")
  dc <- computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetailsReal,
    targetDatabaseSchema = "main",
    targetTable = "cohort_dechal",
    settings = res,
    databaseId = "testing",
    outputFolder = dcLoc,
    minCellCount = 0
  )
  dc <- readr::read_csv(file.path(dcLoc, "dechallenge_rechallenge.csv"), show_col_types = F)
  # one T and 2 Os, so should have 2 rows
  testthat::expect_true(nrow(dc) == 1)
  testthat::expect_true(dc$num_persons_exposed == 4)
  testthat::expect_true(dc$num_exposure_eras == 10)
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
    createTable = T,
    dropTableIfExists = T,
    camelCaseToSnakeCase = F
  )

  res <- createDechallengeRechallengeSettings(
    targetIds = 1,
    outcomeIds = 2,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30 # 31
  )

  dcLoc <- tempfile("runADechal2")
  dc <- computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetailsReal,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = res,
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    databaseId = "testing",
    outputFolder = dcLoc,
    minCellCount = 0
  )

  # person 2 should be in results
  dc <- readr::read_csv(file.path(dcLoc, "rechallenge_fail_case_series.csv"), show_col_types = F)
  testthat::expect_equal(nrow(dc), 1)

  testthat::expect_true(is.na(dc$subject_id))

  dcLoc <- tempfile("runADechal3")
  dc <- computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetailsReal,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = res,
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    databaseId = "testing",
    showSubjectId = T,
    outputFolder = dcLoc,
    minCellCount = 0
  )

  # person 2 should be in results
  dc <- readr::read_csv(file.path(dcLoc, "rechallenge_fail_case_series.csv"), show_col_types = F)
  testthat::expect_equal(nrow(dc), 1)
  testthat::expect_equal(dc$subject_id, 2)


  # check minCellCount
  dcLoc <- tempfile("runADechal4")
  dr <- computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetailsReal,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = res,
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    databaseId = "testing",
    outputFolder = dcLoc,
    minCellCount = 9999
  )

  # checking minCellCount
  # person 2 should be in results but all min cell count
  # values should be censored
  dr <- readr::read_csv(file.path(dcLoc, "dechallenge_rechallenge.csv"), show_col_types = F)
  testthat::expect_true(nrow(dr) > 0)
  testthat::expect_equal(max(dr$num_persons_exposed), -9999)
  testthat::expect_equal(max(dr$num_cases), -9999)
  testthat::expect_equal(max(dr$dechallenge_attempt), -9999)
  testthat::expect_equal(max(dr$dechallenge_fail), -9999)
  testthat::expect_equal(max(dr$dechallenge_success), -9999)
  testthat::expect_equal(max(dr$rechallenge_attempt), -9999)
  testthat::expect_equal(max(dr$rechallenge_fail), -9999)
  testthat::expect_equal(max(dr$rechallenge_success), -9999)
})


# add test for job creation code
test_that("computeDechallengeRechallengeAnalyses", {
  targetIds <- c(2, 5, 6, 7, 8)
  outcomeIds <- c(3, 4, 9, 10)

  res <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )
  jobs <- Characterization:::getDechallengeRechallengeJobs(
    characterizationSettings = createCharacterizationSettings(
      dechallengeRechallengeSettings = res
    ),
    threads = 1
  )

  # as 1 thread should be 2 rows for two analyses
  testthat::expect_true(nrow(jobs) == 2)

  # check all target ids are in there
  targetIdFromSettings <- do.call(
    what = unique,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$targetCohortDefinitionIds
    })
  )
  testthat::expect_true(sum(targetIds %in% targetIdFromSettings) ==
    length(targetIds))

  # check all outcome ids are in there
  outcomeIdFromSettings <- do.call(
    what = unique,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$outcomeCohortDefinitionIds
    })
  )
  testthat::expect_true(sum(outcomeIds %in% outcomeIdFromSettings) ==
    length(outcomeIds))


  # checking more threads 3
  jobs <- Characterization:::getDechallengeRechallengeJobs(
    characterizationSettings = createCharacterizationSettings(
      dechallengeRechallengeSettings = res
    ),
    threads = 3
  )

  # as 3 thread should be 2*3 rows for two analyses
  testthat::expect_true(nrow(jobs) == 2 * 3)

  # check all target ids are in there
  targetIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$targetCohortDefinitionIds
    })
  )
  testthat::expect_true(sum(targetIds %in% targetIdFromSettings) ==
    length(targetIds))

  # check all outcome ids are in there
  outcomeIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$outcomeCohortDefinitionIds
    })
  )
  testthat::expect_true(sum(outcomeIds %in% outcomeIdFromSettings) ==
    length(outcomeIds))



  # checking more threads than needed 20
  jobs <- Characterization:::getDechallengeRechallengeJobs(
    characterizationSettings = createCharacterizationSettings(
      dechallengeRechallengeSettings = res
    ),
    threads = 20
  )

  # as 3 thread should be 2*5 rows for two analyses
  testthat::expect_true(nrow(jobs) == 2 * 5)

  # check all target ids are in there
  targetIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$targetCohortDefinitionIds
    })
  )
  testthat::expect_true(sum(targetIds %in% targetIdFromSettings) ==
    length(targetIds))

  # check all outcome ids are in there
  outcomeIdFromSettings <- do.call(
    what = c,
    args = lapply(1:nrow(jobs), function(i) {
      ParallelLogger::convertJsonToSettings(jobs$settings[i])$outcomeCohortDefinitionIds
    })
  )
  testthat::expect_true(sum(outcomeIds %in% outcomeIdFromSettings) ==
    length(outcomeIds))
})
