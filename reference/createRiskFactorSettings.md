# Create risk factor study settings

Create risk factor study settings

## Usage

``` r
createRiskFactorSettings(
  studyPopulationSettings,
  outcomeIds,
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
    shortTermStartDays = -30)
)
```

## Arguments

- studyPopulationSettings:

  A list of objects created using `createStudyPopulationSettings` that
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

- covariateSettings:

  An object created using
  [`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html)

## Value

A list with the settings

## See also

Other Aggregate:
[`createCaseSeriesSettings()`](createCaseSeriesSettings.md),
[`createTargetBaselineSettings()`](createTargetBaselineSettings.md)

## Examples

``` r

riskFactorSetting <- createRiskFactorSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = c(1,2),
    minPriorObservation = 365,
    limitToFirstInNDays = 99999
  ),
  outcomeIds = c(3),
  outcomeWashoutDays = 90,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start"
)
```
