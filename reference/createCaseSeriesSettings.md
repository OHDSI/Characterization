# Create aggregate covariate study settings

Create aggregate covariate study settings

## Usage

``` r
createCaseSeriesSettings(
  targetIds,
  outcomeIds,
  limitToFirstInNDays = 99999,
  minPriorObservation = 0,
  outcomeWashoutDays = 0,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start",
  caseCovariateSettings = createDuringCovariateSettings(useConditionGroupEraDuring =
    TRUE, useDrugGroupEraDuring = TRUE, useProcedureOccurrenceDuring = TRUE,
    useDeviceExposureDuring = TRUE, useMeasurementDuring = TRUE, useObservationDuring =
    TRUE, useVisitConceptCountDuring = TRUE),
  casePreTargetDuration = 365,
  casePostOutcomeDuration = 365
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

- caseCovariateSettings:

  An object created using `createDuringCovariateSettings`

- casePreTargetDuration:

  The number of days prior to case index we use for FeatureExtraction

- casePostOutcomeDuration:

  The number of days prior to case index we use for FeatureExtraction

## Value

A list with the settings

## See also

Other Aggregate:
[`createRiskFactorSettings()`](createRiskFactorSettings.md),
[`createTargetBaselineSettings()`](createTargetBaselineSettings.md)

## Examples

``` r
caseSeriesSetting <- createCaseSeriesSettings(
  targetIds = c(1,2),
  outcomeIds = c(3),
  limitToFirstInNDays = 365,
  minPriorObservation = 365,
  outcomeWashoutDays = 90,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start",
  casePreTargetDuration = 365,
  casePostOutcomeDuration = 365
)
```
