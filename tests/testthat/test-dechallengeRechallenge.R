# library(Characterization)
# library(testthat)

context("DechallengeRechallenge")

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

  dc <- computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    dechallengeRechallengeSettings = res,
    databaseId = "testing"
  )

  testthat::expect_true(inherits(dc, "Andromeda"))
  testthat::expect_true(names(dc) == "dechallengeRechallenge")

  # check saving/loading
  tempFile <- tempfile(fileext = ".zip")
  on.exit(unlink(tempFile))

  saveDechallengeRechallengeAnalyses(
    result = dc,
    fileName = tempFile
  )

  testthat::expect_true(
    file.exists(tempFile)
  )

  res2 <- loadDechallengeRechallengeAnalyses(
    fileName = tempFile
  )

  testthat::expect_equal(dplyr::collect(dc$dechallengeRechallenge), dplyr::collect(res2$dechallengeRechallenge))

  # check exporting to csv
  tempFolder <- tempfile("exportToCsv")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)
  exportDechallengeRechallengeToCsv(
    result = dc,
    saveDirectory = tempFolder
  )

  countN <- dplyr::pull(dplyr::count(dc$dechallengeRechallenge))

  if (countN > 0) {
    testthat::expect_true(
      file.exists(file.path(tempFolder, "dechallenge_rechallenge.csv"))
    )
  }
})

test_that("computeRechallengeFailCaseSeriesAnalyses with known data", {
  tempDbLoc <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tempDbLoc))
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = tempDbLoc
  )

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

  con <- DatabaseConnector::connect(connectionDetails = connectionDetails)

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

  dc <- computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    dechallengeRechallengeSettings = res,
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    databaseId = "testing"
  )

  # person 2 should be in results

  testthat::expect_equal(dplyr::pull(dplyr::count(dc$rechallengeFailCaseSeries)), 1)

  sub <- dc$rechallengeFailCaseSeries %>%
    dplyr::select("subjectId") %>%
    dplyr::collect()
  testthat::expect_true(is.na(sub$subjectId))

  dc <- computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    dechallengeRechallengeSettings = res,
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    databaseId = "testing",
    showSubjectId = T
  )

  # person 2 should be in results

  testthat::expect_equal(dplyr::pull(dplyr::count(dc$rechallengeFailCaseSeries)), 1)

  sub <- dc$rechallengeFailCaseSeries %>%
    dplyr::select("subjectId") %>%
    dplyr::collect()
  testthat::expect_equal(sub$subjectId, 2)
})
