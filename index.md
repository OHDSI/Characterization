# Characterization

[![Build
Status](https://github.com/OHDSI/Characterization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/Characterization/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/Characterization/coverage.svg?branch=main)](https://codecov.io/github/OHDSI/Characterization?branch=main)
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/Characterization)](https://cran.r-project.org/package=Characterization)
[![CRAN_Status_Badge](https://cranlogs.r-pkg.org/badges/Characterization)](https://cran.r-project.org/package=Characterization)

Characterization is part of [HADES](https://ohdsi.github.io/Hades/).

# Introduction

Characterization is an R package for performing characterization of a
target and a comparator cohort.

# Features

- Compute time to event
- Compute dechallenge and rechallenge
- Computer characterization of target cohort with and without occurring
  in an outcome cohort during some time at risk
- Run multiple characterization analyses efficiently
- upload results to database
- export results as csv files

# Examples

``` r


library(Characterization)

connectionDetails <- Characterization::exampleOmopConnectionDetails()

targetIds <- c(1,2,4)
  outcomeIds <- c(3)

  timeToEventSettings <- createTimeToEventSettings(
    studyPopulationSettings = createStudyPopulationSettings(
     targetIds = c(1,2),
     limitToFirstInNDays = 0,
     minPriorObservation = 0
     ),
    outcomeIds = c(3,4)
  )

  dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
    studyPopulationSettings = createStudyPopulationSettings(
     targetIds = c(1,2),
     limitToFirstInNDays = 0,
     minPriorObservation = 0
     ),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  riskFactorSettings1 <- createRiskFactorSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 99999, # first exposure
      minPriorObservation = 365 # requiring 365 days prior obs
     ),
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 365,
    endAnchor = 'cohort start',
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE,
      useDemographicsAge = TRUE,
      useDemographicsRace = TRUE
    )
  )

  riskFactorSettings2 <- createRiskFactorSettings(
    studyPopulationSettings = createStudyPopulationSettings(
      targetIds = targetIds,
      limitToFirstInNDays = 99999, # first exposure
      minPriorObservation = 365 # requiring 365 days prior obs
     ),
    outcomeIds = outcomeIds,
    riskWindowStart = 1,
    startAnchor = 'cohort start',
    riskWindowEnd = 365,
    endAnchor = 'cohort start',
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useConditionOccurrenceLongTerm = TRUE
    )
  )

  characterizationSettings <- createCharacterizationSettings(
    timeToEventSettings = list(
      timeToEventSettings
      ),
    dechallengeRechallengeSettings = list(
      dechallengeRechallengeSettings
    ),  
    riskFactorSettings =  list(
      riskFactorSettings1,
      riskFactorSettings2
      )
  )
  
runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = 'main',
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  outcomeDatabaseSchema = 'main',
  outcomeTable = 'cohort',
  outputDatabaseSchema = 'main',
  outputTable = 'char_cohort',
  characterizationSettings = characterizationSettings,   
  outputDirectory = file.path(tempdir(), 'example', 'results'),
  executionPath = file.path(tempdir(), 'example', 'execution'),
  csvFilePrefix = 'c_',
  databaseId = 'Eunomia',
  minSMD = 0.1
)
```

# Technology

Characterization is an R package.

# System Requirements

Requires R (version 4.0.0 or higher). Libraries used in Characterization
require Java.

# Installation

1.  See the instructions
    [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring
    your R environment, including Java.

2.  In R, use the following commands to download and install
    Characterization:

``` r

# CRAN
install.packages('Characterization')

# GitHub
install.packages("remotes")
remotes::install_github("ohdsi/Characterization")
```

# User Documentation

Documentation can be found on the [package
website](https://ohdsi.github.io/Characterization).

# Support

- Developer questions/comments/feedback: [OHDSI
  Forum](http://forums.ohdsi.org/c/developers)
- We use the [GitHub issue
  tracker](https://github.com/OHDSI/Characterization/issues) for all
  bugs/issues/enhancements

# Contributing

Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can
contribute to this package.

# License

Characterization is licensed under Apache License 2.0

# Development

Characterization is being developed in R Studio.
