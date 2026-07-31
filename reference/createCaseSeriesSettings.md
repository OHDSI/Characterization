# Create aggregate covariate study settings

Create aggregate covariate study settings

## Usage

``` r
createCaseSeriesSettings(
  studyPopulationSettings,
  outcomeIds,
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

- studyPopulationSettings:

  A List of object created using `createStudyPopulationSettings` that
  specifies target cohorts and inclusion criteria

- outcomeIds:

  A list of cohortIds for the outcome cohorts

- outcomeWashoutDays:

  A single integer value. Patients with the outcome within
  outcomeWashout days prior to index are excluded from the risk factor
  analysis

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

  A single integer value. The number of days prior to case index we use
  for FeatureExtraction

- casePostOutcomeDuration:

  A single integer value. The number of days prior to case index we use
  for FeatureExtraction

## Value

A list with the settings

## See also

Other Aggregate:
[`createRiskFactorSettings()`](createRiskFactorSettings.md),
[`createTargetBaselineSettings()`](createTargetBaselineSettings.md)

## Examples

``` r

caseSeriesSetting <- createCaseSeriesSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = c(1,2),
    minPriorObservation = 365,
    limitToFirstInNDays = 365
  ),
  outcomeIds = c(3),
  outcomeWashoutDays = 90,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start",
  casePreTargetDuration = 365,
  casePostOutcomeDuration = 365
)
```
