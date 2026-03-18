# Create during covariate settings

Create during covariate settings

## Usage

``` r
createDuringCovariateSettings(
  useConditionOccurrenceDuring = FALSE,
  useConditionOccurrencePrimaryInpatientDuring = FALSE,
  useConditionEraDuring = FALSE,
  useConditionGroupEraDuring = FALSE,
  useDrugExposureDuring = FALSE,
  useDrugEraDuring = FALSE,
  useDrugGroupEraDuring = FALSE,
  useProcedureOccurrenceDuring = FALSE,
  useDeviceExposureDuring = FALSE,
  useMeasurementDuring = FALSE,
  useObservationDuring = FALSE,
  useVisitCountDuring = FALSE,
  useVisitConceptCountDuring = FALSE,
  includedCovariateConceptIds = c(),
  addDescendantsToInclude = FALSE,
  excludedCovariateConceptIds = c(),
  addDescendantsToExclude = FALSE,
  includedCovariateIds = c()
)
```

## Arguments

- useConditionOccurrenceDuring:

  One covariate per condition in the condition_occurrence table starting
  between cohort start and cohort end. (analysis ID 109)

- useConditionOccurrencePrimaryInpatientDuring:

  One covariate per condition observed as a primary diagnosis in an
  inpatient setting in the condition_occurrence table starting between
  cohort start and cohort end. (analysis ID 110)

- useConditionEraDuring:

  One covariate per condition in the condition_era table starting
  between cohort start and cohort end. (analysis ID 217)

- useConditionGroupEraDuring:

  One covariate per condition era rolled up to groups in the
  condition_era table starting between cohort start and cohort end.
  (analysis ID 218)

- useDrugExposureDuring:

  One covariate per drug in the drug_exposure table between cohort start
  and end. (analysisId 305)

- useDrugEraDuring:

  One covariate per drug in the drug_era table between cohort start and
  end. (analysis ID 417)

- useDrugGroupEraDuring:

  One covariate per drug rolled up to ATC groups in the drug_era table
  between cohort start and end. (analysis ID 418)

- useProcedureOccurrenceDuring:

  One covariate per procedure in the procedure_occurrence table between
  cohort start and end. (analysis ID 505)

- useDeviceExposureDuring:

  One covariate per device in the device exposure table starting between
  cohort start and end. (analysis ID 605)

- useMeasurementDuring:

  One covariate per measurement in the measurement table between cohort
  start and end. (analysis ID 713)

- useObservationDuring:

  One covariate per observation in the observation table between cohort
  start and end. (analysis ID 805)

- useVisitCountDuring:

  The number of visits observed between cohort start and end. (analysis
  ID 926)

- useVisitConceptCountDuring:

  The number of visits observed between cohort start and end, stratified
  by visit concept ID. (analysis ID 927)

- includedCovariateConceptIds:

  A list of concept IDs that should be used to construct covariates.

- addDescendantsToInclude:

  Should descendant concept IDs be added to the list of concepts to
  include?

- excludedCovariateConceptIds:

  A list of concept IDs that should NOT be used to construct covariates.

- addDescendantsToExclude:

  Should descendant concept IDs be added to the list of concepts to
  exclude?

- includedCovariateIds:

  A list of covariate IDs that should be restricted to.

## Value

An object of type `covariateSettings`, to be used in other functions.

## Details

creates an object specifying how during covariates should be constructed
from data in the CDM model.

## See also

Other CovariateSetting:
[`getDbDuringCovariateData()`](getDbDuringCovariateData.md)

## Examples

``` r
settings <- createDuringCovariateSettings(
  useConditionOccurrenceDuring = TRUE,
  useConditionOccurrencePrimaryInpatientDuring = FALSE,
  useConditionEraDuring = FALSE,
  useConditionGroupEraDuring = FALSE
)
```
