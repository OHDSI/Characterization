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
  testthat::expect_true(
    sum(unique(
      tte$targetCohortDefinitionId
    ) %in% targetIds) ==
      length(unique(tte$targetCohortDefinitionId))
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
