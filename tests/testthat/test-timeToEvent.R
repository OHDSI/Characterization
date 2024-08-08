# library(Characterization)
# library(testthat)

context("TimeToEvent")

test_that("createTimeToEventSettings", {
  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))

  res <- createTimeToEventSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds
  )

  testthat::expect_true(
    length(unique(res$targetIds)) == length(targetIds)
  )

  testthat::expect_true(
    length(unique(res$outcomeIds)) == length(outcomeIds)
  )
})

test_that("computeTimeToEventSettings", {
  targetIds <- c(1, 2)
  outcomeIds <- c(3, 4)

  res <- createTimeToEventSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds
  )

  tteFolder <- tempfile("tte")

  computeTimeToEventAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = res,
    outputFolder = tteFolder,
    databaseId = "tte_test"
  )

  testthat::expect_true(file.exists(file.path(tteFolder, "time_to_event.csv")))

  tte <- readr::read_csv(
    file = file.path(tteFolder, "time_to_event.csv"),
    show_col_types = F
  )

  testthat::expect_true(nrow(tte) == 160)
  testthat::expect_true("database_id" %in% colnames(tte))
  testthat::expect_true(tte$database_id[1] == "tte_test")

  testthat::expect_true(
    length(
      unique(
        tte$target_cohort_definition_id
      )
    ) <= length(targetIds)
  )
  testthat::expect_true(
    sum(unique(
      tte$target_cohort_definition_id
    ) %in% targetIds) ==
      length(unique(tte$target_cohort_definition_id))
  )


  testthat::expect_true(
    length(
      unique(
        tte$outcome_cohort_definition_id
      )
    ) <= length(outcomeIds)
  )
  testthat::expect_true(
    sum(
      unique(tte$outcome_cohort_definition_id)
      %in% outcomeIds
    ) ==
      length(unique(tte$outcome_cohort_definition_id))
  )


  # test minCellCount
  tteFolder <- tempfile("tte2")
  computeTimeToEventAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    settings = res,
    outputFolder = tteFolder,
    databaseId = "tte_test",
    minCellCount = 9999
  )

  tte <- readr::read_csv(
    file = file.path(tteFolder, "time_to_event.csv"),
    show_col_types = F
  )

  testthat::expect_true(max(tte$num_events) == -9999)
})
