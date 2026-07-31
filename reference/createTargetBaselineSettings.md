# Create target baseline aggregate covariate study settings

Create target baseline aggregate covariate study settings

## Usage

``` r
createTargetBaselineSettings(
  studyPopulationSettings,
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
    useVisitConceptCountShortTerm = TRUE, useCharlsonIndex = TRUE, endDays = 0,
    longTermStartDays = -365, shortTermStartDays = -30)
)
```

## Arguments

- studyPopulationSettings:

  An object created using `createStudyPopulationSettings` or a list of
  `createStudyPopulationSettings` that specifies specific populations of
  interest

- covariateSettings:

  An object created using
  [`FeatureExtraction::createCovariateSettings`](https://rdrr.io/pkg/FeatureExtraction/man/createCovariateSettings.html)

## Value

A list with the settings

## See also

Other Aggregate:
[`createCaseSeriesSettings()`](createCaseSeriesSettings.md),
[`createRiskFactorSettings()`](createRiskFactorSettings.md)

## Examples

``` r

aggregateSetting <- createTargetBaselineSettings(
  studyPopulationSettings = createStudyPopulationSettings(
  targetIds = 1:2,
  limitToFirstInNDays = 99999,
  minPriorObservation = 365
  )
)
```
