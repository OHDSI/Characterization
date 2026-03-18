# Create target baseline aggregate covariate study settings

Create target baseline aggregate covariate study settings

## Usage

``` r
createTargetBaselineSettings(
  targetIds,
  limitToFirstInNDays = 99999,
  minPriorObservation = 0,
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

- targetIds:

  A list of cohortIds for the target cohorts

- limitToFirstInNDays:

  Whether to remove target cohort entries that occur within
  limitToFirstInNDays of a prior entry. limitToFirstInNDays = 99999
  means limit to first entry.

- minPriorObservation:

  The minimum time (in days) in the database a patient in the target
  cohorts must be observed prior to index

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
  targetIds = c(1,2),
  limitToFirstInNDays = 99999,
  minPriorObservation = 365
)
```
