# Compute time to event study

Compute time to event study

## Usage

``` r
computeTimeToEventAnalyses(
  connectionDetails = NULL,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema = targetDatabaseSchema,
  outcomeTable = targetTable,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cdmDatabaseSchema,
  settings,
  databaseId = "database 1",
  outputFolder,
  minCellCount = 0,
  progressBar = interactive(),
  executionId,
  ...
)
```

## Arguments

- connectionDetails:

  An object of type \`connectionDetails\` as created using the
  \[DatabaseConnector::createConnectionDetails()\] function.

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

- tempEmulationSchema:

  Some database platforms like Oracle and Impala do not truly support
  temp tables. To emulate temp tables, provide a schema with write
  privileges where temp tables can be created

- cdmDatabaseSchema:

  The database schema containing the OMOP CDM data

- settings:

  The settings for the timeToEvent study

- databaseId:

  An identifier for the database (string)

- outputFolder:

  A directory to save the results as csv files

- minCellCount:

  The minimum cell value to display, values less than this will be
  replaced by -1

- progressBar:

  Whether to display a progress bar while the analysis is running

- executionId:

  a unique id for the run

- ...:

  extra inputs

## Value

An
[`Andromeda::andromeda()`](https://rdrr.io/pkg/Andromeda/man/andromeda_constructor.html)
object containing the time to event results.

## See also

Other TimeToEvent:
[`createTimeToEventSettings()`](createTimeToEventSettings.md)

## Examples

``` r
# example code

conDet <- exampleOmopConnectionDetails()

tteSet <- createTimeToEventSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

result <- computeTimeToEventAnalyses(
  connectionDetails = conDet,
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  cdmDatabaseSchema = 'main',
  settings = tteSet,
  outputFolder = file.path(tempdir(), 'tte')
)
#> Connecting using SQLite driver
#> Uploading #cohort_settings
#> Inserting data took 0.0124 secs
#> Computing time to event results
#> Executing SQL took 0.0314 secs
#> Computing time-to-event for 2 T-O pairs took 0.302 secs
#> exporting to andromeda
#> Disconnected Andromeda. This data object can no longer be used


```
