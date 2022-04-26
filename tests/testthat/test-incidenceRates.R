library(Characterization)
library(testthat)

test_that("Incidence rates", {

  cohort <- data.frame(cohortDefinitionId = c(1, 1, 2),
                       cohortStartDate = as.Date(c("2000-01-01", "2000-01-01", "2000-01-02")),
                       cohortEndDate = as.Date(c("2000-02-01", "2000-02-01", "2000-01-02")),
                       subjectId = c(1, 2, 1))

  DatabaseConnector::insertTable(connection = connection,
                                 databaseSchema = "main",
                                 tableName = "cohort",
                                 data = cohort,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 progressBar = FALSE,
                                 camelCaseToSnakeCase = TRUE)

  targetOutcomes <- data.frame(targetId = 1, outcomeId = 2)

  result <- computeIncidenceRates(connection = connection,
                                  targetDatabaseSchema = "main",
                                  targetTable = "cohort",
                                  outcomeDatabaseSchema = "main",
                                  outcomeTable = "cohort",
                                  targetOutcomes = targetOutcomes,
                                  riskWindowStart = 0,
                                  startAnchor = "cohort start",
                                  riskWindowEnd = 0,
                                  endAnchor = "cohort end")

  expect_equal(result$incidenceRate, 0.5)
})


