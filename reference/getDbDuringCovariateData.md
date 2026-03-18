# Extracts covariates that occur during a cohort

Extracts covariates that occur during a cohort

## Usage

``` r
getDbDuringCovariateData(
  connection,
  oracleTempSchema = NULL,
  cdmDatabaseSchema,
  cdmVersion = "5",
  cohortTable = "#cohort_person",
  rowIdField = "subject_id",
  aggregated = TRUE,
  cohortIds = c(-1),
  covariateSettings,
  minCharacterizationMean = 0,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  targetDatabaseSchema = NULL,
  targetCovariateTable = NULL,
  targetCovariateContinuousTable = NULL,
  targetCovariateRefTable = NULL,
  targetAnalysisRefTable = NULL,
  targetTimeRefTable = NULL,
  ...
)
```

## Arguments

- connection:

  The database connection

- oracleTempSchema:

  The temp schema if using oracle

- cdmDatabaseSchema:

  The schema of the OMOP CDM data

- cdmVersion:

  version of the OMOP CDM data

- cohortTable:

  the table name that contains the target population cohort

- rowIdField:

  string representing the unique identifier in the target population
  cohort

- aggregated:

  whether the covariate should be aggregated

- cohortIds:

  cohort id for the target cohort

- covariateSettings:

  settings for the covariate cohorts and time periods

- minCharacterizationMean:

  The minimum mean value for binary characterization output. Values
  below this will be cut off from output. This will help reduce the file
  size of the characterization output, but will remove information on
  covariates that have very low values. The default is 0.

- tempEmulationSchema:

  Some database platforms like Oracle and Impala do not truly support
  temp tables. To emulate temp tables, provide a schema with write
  privileges where temp tables can be created

- targetDatabaseSchema:

  (Optional) The schema to save the tables
  targetCovariateTable/targetCovariateContinuousTable/targetCovariateRefTable/targetCovariateRefTable/targetAnalysisRefTable
  when they are not temp tables and the output is being exported to
  database tables.

- targetCovariateTable:

  (Optional) The name of the table where the resulting binary covariates
  will be stored. If not provided, results will be fetched to R. The
  table can be a permanent table in the `targetDatabaseSchema` or a temp
  table. If it is a temp table, do not specify `targetDatabaseSchema`.

- targetCovariateContinuousTable:

  (Optional) The name of the table where the resulting continuous
  covariates will be stored. If not provided, results will be fetched
  to R. The table can be a permanent table in the `targetDatabaseSchema`
  or a temp table. If it is a temp table, do not specify
  `targetDatabaseSchema`.

- targetCovariateRefTable:

  (Optional) The name of the table where the covariate reference will be
  stored.

- targetAnalysisRefTable:

  (Optional) The name of the table where the analysis reference will be
  stored.

- targetTimeRefTable:

  (Optional) The name of the table for the time reference

- ...:

  additional arguments from FeatureExtraction

## Value

A 'FeatureExtraction' covariateData object containing the during
covariates based on user settings

## Details

The user specifies a what during covariates they want and this executes
them using FE

## See also

Other CovariateSetting:
[`createDuringCovariateSettings()`](createDuringCovariateSettings.md)

## Examples

``` r
conDet <- exampleOmopConnectionDetails()
connection <- DatabaseConnector::connect(conDet)
#> Connecting using SQLite driver

settings <- createDuringCovariateSettings(
  useConditionOccurrenceDuring = TRUE,
  useConditionOccurrencePrimaryInpatientDuring = FALSE,
  useConditionEraDuring = FALSE,
  useConditionGroupEraDuring = FALSE
)

duringData <- getDbDuringCovariateData(
  connection <- connection,
  cdmDatabaseSchema = 'main',
  cohortIds = 1,
  covariateSettings = settings,
  cohortTable = 'cohort'
)
#> Constructing during cohort covariates
#> Executing SQL took 0.00573 secs
#> Executing SQL took 0.00459 secs
#> Executing during sql code for ConditionOccurrenceDuring
#> Executing SQL took 0.0182 secs
#> Execution took 0.02 secs
#> Extracting covariates
#> Downloading covariates
#> Extracting covariates took 0.19 secs
#> Removing temp covariate tables

DatabaseConnector::disconnect(connection)
```
