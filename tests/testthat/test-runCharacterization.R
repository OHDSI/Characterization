#library(Characterization)
#library(testthat)

context('runCharacterizationAnalyses')

test_that("runCharacterizationAnalyses", {

  targetIds <- c(1,2,4)
  outcomeIds <- c(3)

  timeToEventSettings1 <- createTimeToEventSettings(
    targetIds = 1,
    outcomeIds = c(3,4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    targetIds = 2,
    outcomeIds = c(3,4)
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
    startAnchor = 'cohort start',
    riskWindowEnd = 365,
    endAnchor = 'cohort start',
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
    startAnchor = 'cohort start',
    riskWindowEnd = 365,
    endAnchor = 'cohort start',
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useConditionOccurrenceLongTerm = T
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

  saveLoc <- saveCharacterizationSettings(
    settings = characterizationSettings,
    saveDirectory = tempdir()
    )

  testthat::expect_true(
    file.exists(
      file.path(tempdir(), 'characterizationSettings.json')
      )
    )

  loadedSettings <- loadCharacterizationSettings(
    saveDirectory = saveLoc
    )

  testthat::expect_true(
  length(loadedSettings$timeToEventSettings) ==
    length(characterizationSettings$timeToEventSettings)
  )
  testthat::expect_true(
    length(loadedSettings$dechallengeRechallengeSettings) ==
      length(characterizationSettings$dechallengeRechallengeSettings)
  )
  testthat::expect_true(
    length(loadedSettings$aggregateCovariateSettings) ==
      length(characterizationSettings$aggregateCovariateSettings)
  )

runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = 'main',
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  outcomeDatabaseSchema = 'main',
  outcomeTable = 'cohort',
  characterizationSettings = characterizationSettings,
  saveDirectory = file.path(tempdir(), 'run'),
  tablePrefix = 'c_',
  databaseId = '1'
)

testthat::expect_true(
  file.exists(file.path(tempdir(),'run', 'tracker.csv'))
)
tracker <- CohortGenerator::readCsv(
  file = file.path(tempdir(),'run', 'tracker.csv')
)
testthat::expect_true(
  nrow(tracker) == 5
)

# check the sqlite database here using export to csv

connectionDetailsT <- DatabaseConnector::createConnectionDetails(
  dbms = 'sqlite',
  server = file.path(tempdir(),'run','sqliteCharacterization', 'sqlite')
)

exportDatabaseToCsv(
  connectionDetails = connectionDetailsT,
  resultSchema = 'main',
  targetDialect = 'sqlite',
  tablePrefix = 'c_',
  saveDirectory = file.path(tempdir(),'csv')
)

testthat::expect_true(
  length(dir(file.path(tempdir(),'csv'))) > 0
)

})
