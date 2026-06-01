context("CohortGeneration")


test_that("getCohortJobs", {
  targetIds <- c(1, 2, 4)
  outcomeIds <- c(3)

  timeToEventSettings1 <- createTimeToEventSettings(
    createStudyPopulationSettings(
      targetIds = 1
    ),
    outcomeIds = c(3, 4)
  )
  timeToEventSettings2 <- createTimeToEventSettings(
    createStudyPopulationSettings(
      targetIds = 2
    ),
    outcomeIds = c(3, 4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  targetBaselineSettings1 <- createTargetBaselineSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE
    )
  )

  targetBaselineSettings2 <- createTargetBaselineSettings(
    createStudyPopulationSettings(
      targetIds = targetIds
    ),
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsAge = TRUE,
      useDemographicsRace = TRUE
    )
  )

  riskFactorSettings <- createRiskFactorSettings(
    createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 365,
      minPriorObservation = 365
    ),
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE,
      useDemographicsAge = TRUE,
      useDemographicsRace = TRUE
    )
  )

  caseSeriesSettings <- createCaseSeriesSettings(
    createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 365,
      minPriorObservation = 365
    ),
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    caseCovariateSettings = createDuringCovariateSettings(
      useVisitCountDuring = TRUE,
      useConditionOccurrenceDuring = TRUE
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
    targetBaselineSettings = list(
      targetBaselineSettings1,
      targetBaselineSettings2
    ),
    riskFactorSettings = list(riskFactorSettings),
    caseSeriesSettings = list(caseSeriesSettings)
  )


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 1
  )

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 3)

testthat::expect_true(sum(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds %in% targetIds) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 2
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 6)

testthat::expect_true(sum(unique(c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds, ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 3
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 9)

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
  ) == 3)


jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'Efficient',
  nTargetJobs = 4
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 9)

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
) == 3)



jobs <- getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'CohortIncidence',
  nTargetJobs = 4
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 12)

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
) == 3)


jobs <- Characterization:::getCohortJobs(
  characterizationSettings = characterizationSettings,
  mode = 'PatientLevelPrediction',
  nTargetJobs = 4
)

testthat::expect_true(nrow(jobs$targets) == 6)
testthat::expect_true(nrow(jobs$cases) == 3)
testthat::expect_true(nrow(jobs$jobs) == 12)

testthat::expect_true(sum(unique(
  c(ParallelLogger::convertJsonToSettings(jobs$jobs$settings[1])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[3])$targetIds,
    ParallelLogger::convertJsonToSettings(jobs$jobs$settings[2])$targetIds)) %in% targetIds
) == 3)

})
