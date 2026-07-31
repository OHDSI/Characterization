# Using Characterization Package

## Introduction

This vignette describes how you can use the Characterization package for
various descriptive studies using OMOP CDM data. The Characterization
package currently contains five different types of analyses:

- Target Baseline Covariates: this returns the mean feature value for a
  set of features specified by the user for i) the Target cohort
  populations.
- Risk Factor Covariates: this returns the mean feature value for a set
  of features specified by the user for i) cases: target population
  patients who had the outcome during some user specified time-at-risk
  and ii) non-cases: the Target population patients who did not have the
  outcome during some user specified time-at-risk or just the target
  population (depending on mode).
- Case Series Covariates: this returns the mean feature value for a set
  of features specified by the user for the cases: target population
  patients who had the outcome during some user specified time-at-risk.
  This is done at three different time periods, before target index
  (before), between target index and outcome index (during) and after
  outcome index (after).
- DechallengeRechallenge: this is mainly aimed at investigating whether
  a drug and event are causally related by seeing whether the drug is
  stopped close in time to the event occurrence (dechallenge) and then
  whether the drug is restarted (a rechallenge occurs) and if so,
  whether the event starts again (a failed rechallenge). In this
  analysis, the Target cohorts are the drug users of interest and the
  Outcome cohorts are the medical events you wish to see whether the
  drug may cause. The user must also specify how close in time a drug
  must be stopped after the outcome to be considered a dechallenge and
  how close in time an Outcome must occur after restarting the drug to
  be considered a failed rechallenge).
- Time-to-event: this returns descriptive results showing the timing
  between the target cohort and outcome. This can help identify whether
  the outcome often precedes the target cohort or whether it generally
  comes after.

## Setup

First we need to install the `Characterization` package:

``` r

install.packages("Characterization")
```

and then load it:

``` r

library(Characterization)
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

In this vignette we will show working examples using a sample of the
`Eunomia` R package GI Bleed simulated data. The function
`exampleOmopConnectionDetails` creates a connection details object for a
SQLITE database containing an example observational medical outcomes
partnership (OMOP) common data model (CDM) data in a temporary location.

``` r

connectionDetails <- Characterization::exampleOmopConnectionDetails()
```

## Examples

### Target Baseline Covariates

To run an ‘Target Baseline Covariate’ analysis you need to create a
setting object using `createTargetBaselineSettings`. This requires
specifying:

- studyPopulationSettings created using `createStudyPopulationSettings`
  to define targetIds and population restrictions.
- the covariate settings using
  [`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html)
  or by creating your own custom feature extraction code.

Using the Eunomia data were we previous generated four cohorts, we can
use cohort ids 1,2 and 4 as the targetIds:

``` r

exampleTargetIds <- c(1, 2, 4)
```

If we want to get information on the sex, age at index and Charlson
Comorbidity index we can create the settings using
[`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html):

``` r

exampleCovariateSettings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useCharlsonIndex = TRUE
)
```

If we want to create the aggregate features for all our target cohort
restricted to the first ever target index and those where the patient
was observed for 365 days or more prior to index, we can run:

``` r

exampleTargetBaselineSettings <- createTargetBaselineSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = exampleTargetIds,
    limitToFirstInNDays = 99999,
    minPriorObservation = 365
  ),
  covariateSettings = exampleCovariateSettings
)
```

Next we need to use the `exampleTargetBaselineSettings` as the settings
to `computeTargetBaselineAnalyses`, we need to use the Eunomia
connectionDetails and in Eunomia the OMOP CDM data and cohort table are
in the ‘main’ schema. The cohort table name is ‘cohort’. The following
code will apply the aggregated covariates analysis using the previously
specified settings on the simulated Eunomia data, but we can specify the
`minCharacterizationMean` to exclude covarites with mean values below
0.01, and we must specify the `outputFolder` where the csv results will
be written to.

``` r

runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  outcomeDatabaseSchema = "main",
  outcomeTable = "cohort",
  characterizationSettings = createCharacterizationSettings(
    targetBaselineSettings = exampleTargetBaselineSettings
  ),
  databaseId = "Eunomia",
  outputDatabaseSchema = "main",
  outputTable = 'example_char_cohort', 
  minCharacterizationMean = 0.01,
  outputDirectory = file.path(tempdir(), "example_char", "results"), 
  executionPath = file.path(tempdir(), "example_char", "execution"),
  minCellCount = 10,
  incremental = FALSE,
  nTargetJobs = 1,
  threads = 1
)
```

You can then see the results in the location
`file.path(tempdir(), 'example_char', 'results')` where you will find
csv files.

### Risk Factor Covariates

To run an ‘Risk Factor Covariate’ analysis you need to create a setting
object using `createRiskFactorSettings`. This requires specifying:

- studyPopulationSettings created using `createStudyPopulationSettings`
  to define targetIds and population restrictions.
- one or more outcomeIds (these must be pre-generated in a cohort table)
- the covariate settings using
  [`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html)
  or by creating your own custom feature extraction code.
- the time-at-risk settings
- riskWindowStart
- startAnchor
- riskWindowEnd
- endAnchor

Using the Eunomia data were we previous generated four cohorts, we can
use cohort ids 1,2 and 4 as the targetIds and cohort id 3 as the
outcomeIds:

``` r

exampleTargetIds <- c(1, 2, 4)
exampleOutcomeIds <- 3
```

If we want to get information on the sex, age at index and Charlson
Comorbidity index we can create the settings using
[`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html):

``` r

exampleCovariateSettings <- FeatureExtraction::createCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useCharlsonIndex = TRUE
)
```

If we want to create the aggregate features for all our cases/non-cases
which are target cohorts restricted to those with/without a record of
the outcome 1 day after target cohort start date until 365 days after
target cohort end date with a outcome washout of 9999 (meaning we only
include outcomes that are the first occurrence in the past 9999 days)
and only include targets where the patient was observed for 365 days or
more prior, we can run:

``` r

exampleRiskFactorSettings <- createRiskFactorSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = exampleTargetIds,
    limitToFirstInNDays = 99999, # limit to first target exposure
    minPriorObservation = 365
  ),
  outcomeIds = exampleOutcomeIds,
  riskWindowStart = 1, startAnchor = "cohort start",
  riskWindowEnd = 365, endAnchor = "cohort start",
  outcomeWashoutDays = 9999,
  covariateSettings = exampleCovariateSettings 
)
```

Next we need to use the `exampleRiskFactorSettings` as the settings to
`computeRiskFactorAnalyses`, we need to use the Eunomia
connectionDetails and in Eunomia the OMOP CDM data and cohort table are
in the ‘main’ schema. The cohort table name is ‘cohort’. The following
code will apply the aggregated covariates analysis using the previously
specified settings on the simulated Eunomia data, but we can specify the
`minCharacterizationMean` to exclude covariates with mean values below
0.01, in addition we can specify minSMD to exclude covariates that are
not sufficiently associated to having the outcome and we must specify
the `outputFolder` where the csv results will be written to.

One key input when running risk factors analysis is the mode. There are
currently three supported modes:

- Efficient - in this mode the non-cases are the target population
- CohortIncidence - in this mode the non-cases are the target population
  without the outcome and with \>=1 day of time-at-risk
- PatientLevelPrediction - in this mode the non-cases are the target
  population without the outcome and without the outcome during the
  outcome washout days prior to index.

``` r

runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  outcomeDatabaseSchema = "main",
  outcomeTable = "cohort", 
  outputDatabaseSchema = 'main', 
  outputTable = 'example_char_cohort',
  characterizationSettings = createCharacterizationSettings(
    riskFactorSettings = exampleRiskFactorSettings
  ),
  databaseId = "Eunomia",
  minSMD = 0.1, # only keep moderate to strongly associated covariates
  minCharacterizationMean = 0.01,
  outputDirectory = file.path(tempdir(), "example_char", "results"), 
  executionPath = file.path(tempdir(), "example_char", "execution"),
  minCellCount = 10,
  incremental = FALSE,
  nTargetJobs = 1,
  threads = 1, 
  mode = 'CohortIncidence' # can also pick 'Efficient' and 'PatientLevelPrediction'
)
```

You can then see the results in the location
`file.path(tempdir(), 'example_char', 'results')` where you will find
csv files.

### Case Series Covariates

To run an ‘Case Series Covariate’ analysis you need to create a setting
object using `createCaseSeriesSettings`. This requires specifying:

- studyPopulationSettings created using `createStudyPopulationSettings`
  to define targetIds and population restrictions.
- one or more outcomeIds (these must be pre-generated in a cohort table)
- the case covariate settings using
  [`Characterization::createDuringCovariateSettings`](../reference/createDuringCovariateSettings.md)
  or by creating your own custom feature extraction code.
- the time-at-risk settings
- riskWindowStart
- startAnchor
- riskWindowEnd
- endAnchor
- the casePreTargetDuration which is the time before target index to
  characterize
- the casePostOutcomeDuration which is the time after outcome index to
  characterize

Using the Eunomia data were we previous generated four cohorts, we can
use cohort ids 1,2 and 4 as the targetIds and cohort id 3 as the
outcomeIds:

``` r

exampleTargetIds <- c(1, 2, 4)
exampleOutcomeIds <- 3
```

If we want to get information on the conditions and visit counts:

``` r

exampleCaseCovariateSettings <- Characterization::createDuringCovariateSettings(
  useConditionOccurrenceDuring = TRUE, 
  useVisitCountDuring = TRUE
  )
```

We also need to specify two variables `casePreTargetDuration` which is
the number of days before target index to extract features for the cases
(answers what happens shortly before the target index) and
`casePostOutcomeDuration` which is the number of days after the outcome
date to extract features for the cases (answers what happens after the
outcome). The case covariates are also extracted between target index
and outcome (answers the question what happens during target exposure).

``` r

exampleCaseSeriesSettings <- createCaseSeriesSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = exampleTargetIds,
    limitToFirstInNDays = 99999, # limit to first target index
    minPriorObservation = 365
  ),
  outcomeIds = exampleOutcomeIds,
  riskWindowStart = 1, startAnchor = "cohort start",
  riskWindowEnd = 365, endAnchor = "cohort start",
  outcomeWashoutDays = 9999,
  caseCovariateSettings = exampleCaseCovariateSettings,
  casePreTargetDuration = 90,
  casePostOutcomeDuration = 90
)
```

Next we need to use the `exampleCaseSeriesSettings` as the settings to
`computeCaseSeriesAnalyses`, we need to use the Eunomia
connectionDetails and in Eunomia the OMOP CDM data and cohort table are
in the ‘main’ schema. The cohort table name is ‘cohort’. The following
code will apply the aggregated covariates analysis using the previously
specified settings on the simulated Eunomia data, but we can specify the
`minCharacterizationMean` to exclude covarites with mean values below
0.01, and we must specify the `outputFolder` where the csv results will
be written to.

``` r

runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  outcomeDatabaseSchema = "main",
  outcomeTable = "cohort",
  
  outputDatabaseSchema = "main",
  outputTable = 'example_char_cohort',
  characterizationSettings = createCharacterizationSettings(
    caseSeriesSettings = exampleCaseSeriesSettings
  ),
  databaseId = "Eunomia",
  minCharacterizationMean = 0.01,
  minCovariateCount = 2, 
  outputDirectory = file.path(tempdir(), "example_char", "results"), 
  executionPath = file.path(tempdir(), "example_char", "execution"),
  minCellCount = 10,
  incremental = FALSE,
  nTargetJobs = 1,
  threads = 1
)
```

You can then see the results in the location
`file.path(tempdir(), 'example_char', 'results')` where you will find
csv files.

### Dechallenge Rechallenge

To run a ‘Dechallenge Rechallenge’ analysis you need to create a setting
object using `createDechallengeRechallengeSettings`. This requires
specifying:

- studyPopulationSettings created using `createStudyPopulationSettings`
  to define targetIds and population restrictions.
- one or more outcomeIds (these must be pre-generated in a cohort table)
- dechallengeStopInterval
- dechallengeEvaluationWindow

Using the Eunomia data were we previous generated four cohorts, we can
use cohort ids 1,2 and 4 as the targetIds and cohort id 3 as the
outcomeIds:

``` r

exampleTargetIds <- c(1, 2, 4)
exampleOutcomeIds <- 3
```

If we want to create the dechallenge rechallenge for all our target
cohorts and our outcome cohort with a 30 day dechallengeStopInterval and
31 day dechallengeEvaluationWindow:

``` r

exampleDechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = exampleTargetIds
  ),
  outcomeIds = exampleOutcomeIds,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 31
)
```

We can then run the analysis on the Eunomia data using
`computeDechallengeRechallengeAnalyses` and the settings previously
specified, with `minCellCount` removing values less than the specified
value:

``` r

dc <- computeDechallengeRechallengeAnalyses(
  connectionDetails = connectionDetails,
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  settings = exampleDechallengeRechallengeSettings,
  databaseId = "Eunomia",
  outcomeFolder = file.path(tempdir(), "example_char", "results"),
  minCellCount = 5
)
```

Next it is possible to compute the failed rechallenge cases

``` r

failed <- computeRechallengeFailCaseSeriesAnalyses(
  connectionDetails = connectionDetails,
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  settings = exampleDechallengeRechallengeSettings,
  outcomeDatabaseSchema = "main",
  outcomeTable = "cohort",
  databaseId = "Eunomia",
  outcomeFolder = file.path(tempdir(), "example_char", "results"),
  minCellCount = 5
)
```

### Time to Event

To run a ‘Time-to-event’ analysis you need to create a setting object
using `createTimeToEventSettings`. This requires specifying:

- studyPopulationSettings created using `createStudyPopulationSettings`
  to define targetIds and population restrictions.
- one or more outcomeIds (these must be pre-generated in a cohort table)

``` r

exampleTimeToEventSettings <- createTimeToEventSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = exampleTargetIds
  ),
  outcomeIds = exampleOutcomeIds
)
```

We can then run the analysis on the Eunomia data using
`computeTimeToEventAnalyses` and the settings previously specified:

``` r

tte <- computeTimeToEventAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  settings = exampleTimeToEventSettings,
  databaseId = "Eunomia",
  outcomefolder = file.path(tempdir(), "example_char", "results"),
  minCellCount = 5
)
```

### Run Multiple

If you want to run multiple analyses (of the three previously shown) you
can use `createCharacterizationSettings`. You need to input a list of
each of the settings (or NULL if you do not want to run one type of
analysis). To run all the analyses previously shown in one function:

``` r

characterizationSettings <- createCharacterizationSettings(
  timeToEventSettings = list(
    exampleTimeToEventSettings
  ),
  dechallengeRechallengeSettings = list(
    exampleDechallengeRechallengeSettings
  ),
  aggregateCovariateSettings = exampleAggregateCovariateSettings
)

# save the settings using
saveCharacterizationSettings(
  settings = characterizationSettings,
  saveDirectory = file.path(tempdir(), "saveSettings")
)

# the settings can be loaded
characterizationSettings <- loadCharacterizationSettings(
  saveDirectory = file.path(tempdir(), "saveSettings")
)

runCharacterizationAnalyses(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = "main",
  targetDatabaseSchema = "main",
  targetTable = "cohort",
  outcomeDatabaseSchema = "main",
  outcomeTable = "cohort",
  characterizationSettings = characterizationSettings,
  outputDirectory = file.path(tempdir(), "example", "results"),
  executionPath = file.path(tempdir(), "example", "execution"),
  csvFilePrefix = "c_",
  databaseId = "1",
  incremental = FALSE,
  minCharacterizationMean = 0.01,
  minCellCount = 5
)
```

This will create csv files with the results in the saveDirectory. You
can run the following code to view the results in a shiny app:

``` r

viewCharacterization(
  resultFolder = file.path(tempdir(), "example", "results"),
  cohortDefinitionSet = NULL
)
```
