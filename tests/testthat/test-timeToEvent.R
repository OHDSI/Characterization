#library(Characterization)
#library(testthat)

context('TimeToEvent')

test_that("createTimeToEventSettings", {

  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))

  res <- createTimeToEventSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds
    )

  testthat::expect_true(
    nrow(res$pairs) == length(targetIds)*length(outcomeIds)
    )

  testthat::expect_true(
    length(unique(res$pairs$targetCohortDefinitionId)) == length(targetIds)
  )

  testthat::expect_true(
    length(unique(res$pairs$outcomeCohortDefinitionId)) == length(outcomeIds)
  )

})

test_that("computeTimeToEventSettings", {

  targetIds <- c(1,2)
  outcomeIds <- c(3,4)

  res <- createTimeToEventSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds
  )

  tte <- computeTimeToEventAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = 'main',
    targetDatabaseSchema = 'main',
    targetTable = 'cohort',
    timeToEventSettings = res
    )

  testthat::expect_true(class(tte) == 'Andromeda')
  testthat::expect_true(names(tte) == 'timeToEvent')

  testthat::expect_true(
    nrow(
      unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select(.data$targetCohortDefinitionId)
        )
      ) <= length(targetIds)
  )
  testthat::expect_true(
    sum(unique(
      tte$timeToEvent %>%
        dplyr::collect() %>%
        dplyr::select(.data$targetCohortDefinitionId)
    )$targetCohortDefinitionId %in% targetIds) ==
      nrow(unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select(.data$targetCohortDefinitionId)
      ))
  )


  testthat::expect_true(
    nrow(
      unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select(.data$outcomeCohortDefinitionId)
      )
    ) <= length(outcomeIds)
  )
  testthat::expect_true(
    sum(unique(
      tte$timeToEvent %>%
        dplyr::collect() %>%
        dplyr::select(.data$outcomeCohortDefinitionId)
    )$outcomeCohortDefinitionId %in% outcomeIds) ==
      nrow(unique(
        tte$timeToEvent %>%
          dplyr::collect() %>%
          dplyr::select(.data$outcomeCohortDefinitionId)
      ))
  )


  # saving
  if(actions){ # causing *** caught segfault *** address 0x68, cause 'memory not mapped' zip::zipr

  saveTimeToEventAnalyses(
    result = tte,
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_true(
    file.exists(file.path(tempdir(), 'TimeToEvent'))
  )

  tte2 <- loadTimeToEventAnalyses(
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_equal(
    nrow(tte$timeToEvent %>% dplyr::collect()),
    nrow(tte2$timeToEvent %>% dplyr::collect())
    )
  }

  # test csv export

  exportTimeToEventToCsv(
    result = tte,
    saveDirectory = tempdir()
  )

  testthat::expect_true(
    file.exists(file.path(
      tempdir(),
      'time_to_event.csv'
    )
    )
  )

  # check it saved correctly and uses snake case
  res <- CohortGenerator::readCsv(
    file =
      file.path(
    tempdir(),
    'time_to_event.csv'
  ),
  col_names = T,
  )
  testthat::expect_true(nrow(res) == 160)
  testthat::expect_true(colnames(res)[1] == 'database_id')

})


