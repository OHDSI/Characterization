# execute a large-scale characterization study

Specify the database connection containing the CDM data, the cohort
database schemas/tables, the characterization settings and the directory
to save the results to

## Usage

``` r
runCharacterizationAnalyses(
  connectionDetails,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema,
  outcomeTable,
  outputDatabaseSchema = targetDatabaseSchema,
  outputTable = "characterization_cohort",
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cdmDatabaseSchema,
  characterizationSettings,
  outputDirectory,
  executionPath = file.path(outputDirectory, "execution"),
  csvFilePrefix = "c_",
  databaseId = "1",
  showSubjectId = FALSE,
  minCellCount = 0,
  incremental = TRUE,
  threads = 1,
  cohortGenerationThreads = NULL,
  nTargetJobs = 1,
  minCharacterizationMean = 0.01,
  minCovariateCount = 0,
  mode = "CohortIncidence",
  minSMD = 0
)
```

## Arguments

- connectionDetails:

  The connection details to the database containing the OMOP CDM data

- targetDatabaseSchema:

  Schema name where your target cohort table resides. Note that for SQL
  Server, this should include both the database and schema name, for
  example 'scratch.dbo'.

- targetTable:

  Name of the target cohort table.

- outcomeDatabaseSchema:

  Schema name where your outcome cohort table resides. Note that for SQL
  Server, this should include both the database and schema name, for
  example 'scratch.dbo'.

- outcomeTable:

  Name of the outcome cohort table.

- outputDatabaseSchema:

  The schema where the characterization cohort table will be saved into

- outputTable:

  The table name where the characterization cohort table will be saved
  into

- tempEmulationSchema:

  Some database platforms like Oracle and Impala do not truly support
  temp tables. To emulate temp tables, provide a schema with write
  privileges where temp tables can be created

- cdmDatabaseSchema:

  The schema with the OMOP CDM data

- characterizationSettings:

  The study settings created using `createCharacterizationSettings`

- outputDirectory:

  The location to save the final csv files to

- executionPath:

  The location where intermediate results are saved to

- csvFilePrefix:

  A string to append the csv files in the outputDirectory

- databaseId:

  The unique identifier for the cdm database

- showSubjectId:

  Whether to include subjectId of failed rechallenge case series or hide

- minCellCount:

  The minimum count value that is calculated

- incremental:

  If TRUE then skip previously executed analyses that completed

- threads:

  The number of threads to use when running analyses jobs in parallel

- cohortGenerationThreads:

  (optional) The number of threads to use when generating the cohorts in
  parallel (Note: some database management systems do not allow insert
  parallelization)

- nTargetJobs:

  Partition the targets into this number of groups (e.g., if there are
  20 targets and njobs is 5 then there will be 4 targets per job and 5
  jobs)

- minCharacterizationMean:

  The minimum mean threshold to extract when running aggregate
  covariates

- minCovariateCount:

  The minimum number of patients who must have the covariate when
  running aggregate covariates

- mode:

  Select from Efficient (no exclusions to target based on
  washout)/CohortIncidence (excludes targets with outcome in washout if
  they have no time at risk)/PatientLevelPrediction (excludes targets
  with outcome during washout prior to index)

- minSMD:

  The minimum standardized mean difference for the risk factor analysis

## Value

Multiple csv files in the outputDirectory.

## Details

The results of the characterization will be saved into an sqlite
database inside the specified saveDirectory

## See also

Other LargeScale:
[`createCharacterizationSettings()`](createCharacterizationSettings.md),
[`loadCharacterizationSettings()`](loadCharacterizationSettings.md),
[`saveCharacterizationSettings()`](saveCharacterizationSettings.md)

## Examples

``` r
conDet <- exampleOmopConnectionDetails()

tteSet <- createTimeToEventSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

cSet <- createCharacterizationSettings(
  timeToEventSettings = tteSet
)

runCharacterizationAnalyses(
  connectionDetails = conDet,
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  outcomeDatabaseSchema = 'main',
  outcomeTable = 'cohort',
  cdmDatabaseSchema = 'main',
  characterizationSettings = cSet,
  outputDirectory = file.path(tempdir(),'runChar')
)
#> Creating directory /tmp/RtmpMCWc5Q/runChar
#> Creating directory /tmp/RtmpMCWc5Q/runChar/execution
#> Currently in a tryCatch or withCallingHandlers block, so unable to add global calling handlers. ParallelLogger will not capture R messages, errors, and warnings, only explicit calls to ParallelLogger. (This message will not be shown again this R session)
#> Connecting using SQLite driver
#> Extracting cohort jobs
#> Connecting using SQLite driver
#> Creating new cluster
#> Connecting using SQLite driver
#> Uploading #cohort_settings
#> Inserting data took 0.0296 secs
#> Computing time to event results
#> Executing SQL took 0.0203 secs
#> Computing time-to-event for 2 T-O pairs took 0.236 secs
#> exporting to andromeda
#> Disconnected Andromeda. This data object can no longer be used
#> Removing any existing results in outputFolder directory
#> Loading andromeda result at /tmp/RtmpMCWc5Q/runChar/execution/tte_1
```
