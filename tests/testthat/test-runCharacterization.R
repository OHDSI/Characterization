# library(Characterization)
# library(testthat)

context("runCharacterizationAnalyses")

test_that("runCharacterizationAnalyses", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)

  timeToEventSettings1 <- createTimeToEventSettings(
    targetIds = 1,
    outcomeIds = c(3, 4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    targetIds = 2,
    outcomeIds = c(3, 4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  aggregateCovariateSettings1 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsRace = T
    )
  )

  aggregateCovariateSettings2 <- createAggregateCovariateSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsRace = T
    )
  )

  characterizationSettings <- createCharacterizationSettings(
    timeToEventSettings = list(
      timeToEventSettings1,
      timeToEventSettings2
    ),
    dechallengeRechallengeSettings = list(
      dechallengeRechallengeSettings
    ),
    aggregateCovariateSettings = list(
      aggregateCovariateSettings1,
      aggregateCovariateSettings2
    )
  )

  testthat::expect_true(
    class(characterizationSettings) == "characterizationSettings"
  )

  testthat::expect_true(
    length(characterizationSettings$timeToEventSettings) == 2
  )
  testthat::expect_true(
    length(characterizationSettings$dechallengeRechallengeSettings) == 1
  )
  testthat::expect_true(
    length(characterizationSettings$aggregateCovariateSettings) == 2
  )

  tempFile <- tempfile(fileext = ".json")
  on.exit(unlink(tempFile))
  saveLoc <- saveCharacterizationSettings(
    settings = characterizationSettings,
    fileName = tempFile
  )

  testthat::expect_true(file.exists(tempFile))

  loadedSettings <- loadCharacterizationSettings(
    fileName = tempFile
  )

  # In R, empty arrays are automatically of type 'logical.' When loading JSON
  # they are currently automatically of type 'list'. Neither is right or wrong,
  # so ignoring distinction:
  convertEmptyListToEmptyLogical <- function(object) {
    if (is.list(object)) {
      if (length(object) == 0) {
        return(vector(mode = "logical", length = 0))
      } else {
        return(lapply(object, convertEmptyListToEmptyLogical))
      }
    } else {
      return(object)
    }
  }
  testthat::expect_equivalent(characterizationSettings, convertEmptyListToEmptyLogical(loadedSettings))

  tempFolder <- tempfile("Characterization")
  on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

  runCharacterizationAnalyses(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = "main",
    targetDatabaseSchema = "main",
    targetTable = "cohort",
    outcomeDatabaseSchema = "main",
    outcomeTable = "cohort",
    characterizationSettings = characterizationSettings,
    outputDirectory = file.path(tempFolder,'result'),
    executionPath = file.path(tempFolder,'execution'),
    csvFilePrefix = "c_",
    databaseId = "1",
    incremental = T,
    minCharacterizationMean = 0.01,
    threads = 1
  )

  testthat::expect_true(
    dir.exists(file.path(tempFolder, "result"))
  )

  # check csv files
  testthat::expect_true(
    length(dir(file.path(tempFolder, "result"))) > 0
  )

  # check cohort details is saved
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_cohort_details.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_settings.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_analysis_ref.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_covariate_ref.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_covariates.csv"))
  )
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_covariates_continuous.csv"))
  )

  # no results for dechal due to Eunomia - how to test?
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_dechallenge_rechallenge.csv"))
  )
  #testthat::expect_true(
  #  file.exists(file.path(tempFolder, "result", "rechallenge_fail_case_series.csv"))
  #)
  testthat::expect_true(
    file.exists(file.path(tempFolder, "result", "c_time_to_event.csv"))
  )

  # make sure both tte runs are in the csv
  tte <- readr::read_csv(
         file = file.path(tempFolder,'result' ,"c_time_to_event.csv"),
         show_col_types = FALSE
     )
  testthat::expect_equivalent(
    unique(tte$target_cohort_definition_id),
    c(1,2)
  )


})
