# Create risk factor study settings

Create risk factor study settings

## Usage

``` r
createRiskFactorSettings(
  targetIds,
  outcomeIds,
  limitToFirstInNDays = 99999,
  minPriorObservation = 0,
  outcomeWashoutDays = 0,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start",
  covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsGender =
    TRUE, useDemographicsAge = TRUE, useDemographicsAgeGroup = TRUE, useDemographicsRace
    = TRUE, useDemographicsEthnicity = TRUE, useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE, useDemographicsTimeInCohort = TRUE,
    useDemographicsPriorObservationTime = TRUE, useDemographicsPostObservationTime =
    TRUE, useConditionGroupEraLongTerm = TRUE, useDrugGroupEraOverlapping = TRUE,
    useDrugGroupEraLongTerm = TRUE, useProcedureOccurrenceLongTerm = TRUE, 
    
    useMeasurementLongTerm = TRUE, useObservationLongTerm = TRUE,
    useDeviceExposureLongTerm = TRUE, useVisitConceptCountLongTerm = TRUE,
    useConditionGroupEraShortTerm = TRUE, useDrugGroupEraShortTerm = TRUE,
    useProcedureOccurrenceShortTerm = TRUE, useMeasurementShortTerm = TRUE,
    useObservationShortTerm = TRUE, useDeviceExposureShortTerm = TRUE,
    useVisitConceptCountShortTerm = TRUE, endDays = 0, longTermStartDays = -365,
    shortTermStartDays = -30),
  minTargetSize = 0,
  minTwithOSize = 0
)
```

## Arguments

- targetIds:

  A list of cohortIds for the target cohorts

- outcomeIds:

  A list of cohortIds for the outcome cohorts

- limitToFirstInNDays:

  whether to limit each target cohort to the first entry into the cohort
  per N days per subject

- minPriorObservation:

  The minimum time (in days) in the database a patient in the target
  cohorts must be observed prior to index

- outcomeWashoutDays:

  Patients with the outcome within outcomeWashout days prior to index
  are excluded from the risk factor analysis

- riskWindowStart:

  The start of the risk window (in days) relative to the
  \`startAnchor\`.

- startAnchor:

  The anchor point for the start of the risk window. Can be \`"cohort
  start"\` or \`"cohort end"\`.

- riskWindowEnd:

  The end of the risk window (in days) relative to the \`endAnchor\`.

- endAnchor:

  The anchor point for the end of the risk window. Can be \`"cohort
  start"\` or \`"cohort end"\`.

- covariateSettings:

  An object created using
  [`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html)

- minTargetSize:

  The minimum size of the target cohorts for them to have aggregate
  covariates calculated

- minTwithOSize:

  The minimum size of the cohorts corresponding to patients in the
  target with the outcome during time-at-risk for them to have aggregate
  covariates calculated

## Value

A list with the settings

## See also

Other Aggregate:
[`createCaseSeriesSettings()`](createCaseSeriesSettings.md),
[`createTargetBaselineSettings()`](createTargetBaselineSettings.md)

## Examples

``` r

riskFactorSetting <- createRiskFactorSettings(
  targetIds = c(1,2),
  outcomeIds = c(3),
  minPriorObservation = 365,
  outcomeWashoutDays = 90,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start"
)
```
