library(Characterization)
library(testthat)

test_that("Create analysis specs and save and load them", {
  targetOutcomes <- data.frame(targetId = 1, comparatorId = 2)

  args <- createComputeIncidenceRatesArgs(targetOutcomes = targetOutcomes)

  analysisList <- createCharacterizationAnalysisList(args, args)

  tempFile <- tempfile(pattern = "charAnalyes", fileext = ".json")
  saveCharacterizationAnalysisList(analysisList, tempFile)

  # json <- SqlRender::readSql(tempFile)
  # writeLines(json)

  analysisList2 <- loadCharacterizationAnalysisList(tempFile)
  unlink(tempFile)

  expect_equal(analysisList, analysisList2)
})


test_that("Create analysis specs and run them", {
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

  targetOutcomes <- data.frame(targetId = 1, comparatorId = 2)

  args <- createComputeIncidenceRatesArgs(targetOutcomes = targetOutcomes)

  analysisList <- createCharacterizationAnalysisList(args)


  tempFolder <- tempfile("charExport")
  dir.create(tempFolder)

  result <- runCharacterizationAnalyses(connectionDetails = connectionDetails,
                                        targetDatabaseSchema = "main",
                                        targetTable = "cohort",
                                        comparatorDatabaseSchema = "main",
                                        comparatorTable = "cohort",
                                        characterizationAnalysisList = analysisList,
                                        exportFolder = tempFolder)
  incidenceRates <- readr::read_csv(file.path(tempFolder, "incidence_rate.csv"), show_col_types = FALSE)

  expect_equal(incidenceRates$incidence_rate, 0.5)

  unlink(tempFolder, recursive = TRUE)
})
