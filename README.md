Characterization
================

[![Build Status](https://github.com/OHDSI/Characterization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/Characterization/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/Characterization/coverage.svg?branch=main)](https://codecov.io/github/OHDSI/Characterization?branch=main)
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/Characterization)](https://cran.r-project.org/package=Characterization)
[![CRAN_Status_Badge](https://cranlogs.r-pkg.org/badges/Characterization)](https://cran.r-project.org/package=Characterization)

Characterization is part of [HADES](https://ohdsi.github.io/Hades/).


Introduction
============
Characterization is an R package for performing characterization of a target and a comparator cohort.

Features
========
- Compute time to event
- Compute dechallenge and rechallenge 
- Computer characterization of target cohort with and without occurring in an outcome cohort during some time at risk
- Run multiple characterization analyses efficiently 
- upload results to database
- export results as csv files

Examples
========

```r

library(Characterization)

connectionDetails <- Characterization::exampleOmopConnectionDetails()

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
  
runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = 'main',
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  outcomeDatabaseSchema = 'main',
  outcomeTable = 'cohort',
  characterizationSettings = characterizationSettings,   
  outputDirectory = file.path(tempdir(), 'example', 'results'),
  executionPath = file.path(tempdir(), 'example', 'execution'),
  csvFilePrefix = 'c_',
  databaseId = 'Eunomia'
)
```

Technology
============
Characterization is an R package.

System Requirements
============
Requires R (version 4.0.0 or higher). Libraries used in Characterization require Java.

Installation
=============
1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including Java.

2. In R, use the following commands to download and install Characterization:

  ```r
  install.packages("remotes")
  remotes::install_github("ohdsi/Characterization")
  ```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/Characterization).

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/Characterization/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
Characterization is licensed under Apache License 2.0

Development
===========
Characterization is being developed in R Studio.
