# Compute dechallenge rechallenge study

Compute dechallenge rechallenge study

## Usage

``` r
computeDechallengeRechallengeAnalyses(
  connectionDetails = NULL,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema = targetDatabaseSchema,
  outcomeTable = targetTable,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  settings,
  databaseId = "database 1",
  outputFolder,
  minCellCount = 0,
  progressBar = interactive(),
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

- ...:

  extra inputs

## Value

An
[`Andromeda::andromeda()`](https://rdrr.io/pkg/Andromeda/man/andromeda_constructor.html)
object containing the dechallenge rechallenge results

## See also

Other DechallengeRechallenge:
[`computeRechallengeFailCaseSeriesAnalyses()`](computeRechallengeFailCaseSeriesAnalyses.md),
[`createDechallengeRechallengeSettings()`](createDechallengeRechallengeSettings.md)

## Examples

``` r

conDet <- exampleOmopConnectionDetails()

drSet <- createDechallengeRechallengeSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

computeDechallengeRechallengeAnalyses(
  connectionDetails = conDet,
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  settings = drSet,
  outputFolder = tempdir()
)
#> Inputs checked
#> Connecting using SQLite driver
#> Computing dechallenge rechallenge results
#> Executing SQL took 0.0258 secs
#> Computing dechallenge rechallenge for 2 target ids and 1 outcome ids took 0.478 secs
#> exporting to andomeda
#> Disconnected Andromeda. This data object can no longer be used

```
