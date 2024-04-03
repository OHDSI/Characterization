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

  tte <- computeTimeToEventAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    timeToEventSettings = res
  )

  testthat::expect_true(class(tte) == "Andromeda")
  testthat::expect_true(names(tte) == "timeToEvent")

  testthat::expect_true(
    nrow(
      unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select("targetCohortDefinitionId")
      )
    ) <= length(targetIds)
  )
  testthat::expect_true(
    sum(unique(
      tte$timeToEvent %>%
        dplyr::collect() %>%
        dplyr::select("targetCohortDefinitionId")
    )$targetCohortDefinitionId %in% targetIds) ==
      nrow(unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select("targetCohortDefinitionId")
      ))
  )


  testthat::expect_true(
    nrow(
      unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select("outcomeCohortDefinitionId")
      )
    ) <= length(outcomeIds)
  )
  testthat::expect_true(
    sum(unique(
      tte$timeToEvent %>%
        dplyr::collect() %>%
        dplyr::select("outcomeCohortDefinitionId")
    )$outcomeCohortDefinitionId %in% outcomeIds) ==
      nrow(unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select("outcomeCohortDefinitionId")
      ))
  )

  # saving
  tempFile <- tempfile(fileext = ".zip")
  on.exit(unlink(tempFile))

  saveTimeToEventAnalyses(
    result = tte,
    fileName = tempFile
  )

  testthat::expect_true(file.exists(tempFile))

  tte2 <- loadTimeToEventAnalyses(tempFile)

  testthat::expect_equal(dplyr::collect(tte$timeToEvent), dplyr::collect(tte2$timeToEvent))

  # test csv export
  tempFolder <- tempfile("exportToCsv")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

  exportTimeToEventToCsv(
    result = tte,
    saveDirectory = tempFolder
  )

  testthat::expect_true(file.exists(file.path(tempFolder, "time_to_event.csv")))

  # check it saved correctly and uses snake case
  res <- readr::read_csv(
    file =
      file.path(
        tempFolder,
        "time_to_event.csv"
      ),
    col_names = T
  )
  testthat::expect_true(nrow(res) == 160)
  testthat::expect_true(colnames(res)[1] == "database_id")
})
