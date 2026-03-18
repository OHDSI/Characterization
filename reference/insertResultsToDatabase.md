# Upload the results into a result database

This function uploads results in csv format into a result database

## Usage

``` r
insertResultsToDatabase(
  connectionDetails,
  schema,
  resultsFolder,
  tablePrefix = "",
  csvTablePrefix = "c_",
  includedFiles = NULL
)
```

## Arguments

- connectionDetails:

  The connection details to the result database

- schema:

  The schema for the result database

- resultsFolder:

  The folder containing the csv results

- tablePrefix:

  A prefix to append to the result tables for the characterization
  results

- csvTablePrefix:

  The prefix added to the csv results - default is 'c\_'

- includedFiles:

  Specify the csv files to upload or NULL to upload all in directory

## Value

Returns the connection to the sqlite database

## Details

Calls ResultModelManager uploadResults function to upload the csv files

## See also

Other Database:
[`createCharacterizationTables()`](createCharacterizationTables.md),
[`createSqliteDatabase()`](createSqliteDatabase.md)

## Examples

``` r
# generate results into resultsFolder
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
  outputDirectory = file.path(tempdir(),'database')
)
#> Creating directory /tmp/RtmpdKM5Qo/database
#> Creating directory /tmp/RtmpdKM5Qo/database/execution
#> Currently in a tryCatch or withCallingHandlers block, so unable to add global calling handlers. ParallelLogger will not capture R messages, errors, and warnings, only explicit calls to ParallelLogger. (This message will not be shown again this R session)
#> Connecting using SQLite driver
#> Extracting cohort jobs
#> Connecting using SQLite driver
#> Creating new cluster
#> Connecting using SQLite driver
#> Uploading #cohort_settings
#> Inserting data took 0.0373 secs
#> Computing time to event results
#> Executing SQL took 0.0217 secs
#> Computing time-to-event for 2 T-O pairs took 0.246 secs
#> exporting to andromeda
#> Disconnected Andromeda. This data object can no longer be used
#> Removing any existing results in outputFolder directory
#> Loading andromeda result at /tmp/RtmpdKM5Qo/database/execution/tte_1

# create sqlite database
charResultDbCD <- createSqliteDatabase()

# create database results tables
createCharacterizationTables(
   connectionDetails = charResultDbCD,
   resultSchema = 'main'
 )
#> Connecting using SQLite driver
#> Deleting existing tables
#> Executing SQL took 0.00267 secs
#> Executing SQL took 0.0027 secs
#> Executing SQL took 0.0029 secs
#> Executing SQL took 0.00269 secs
#> Executing SQL took 0.00259 secs
#> Executing SQL took 0.00278 secs
#> Executing SQL took 0.00265 secs
#> Executing SQL took 0.0028 secs
#> Executing SQL took 0.00266 secs
#> Executing SQL took 0.00269 secs
#> Executing SQL took 0.00253 secs
#> Executing SQL took 0.00257 secs
#> Executing SQL took 0.00261 secs
#> Executing SQL took 0.00262 secs
#> Executing SQL took 0.00253 secs
#> Executing SQL took 0.00261 secs
#> Executing SQL took 0.00264 secs
#> Executing SQL took 0.00279 secs
#> Executing SQL took 0.00252 secs
#> Executing SQL took 0.00291 secs
#> Executing SQL took 0.00255 secs
#> Executing SQL took 0.00262 secs
#> Executing SQL took 0.00251 secs
#> Executing SQL took 0.00256 secs
#> Executing SQL took 0.00254 secs
#> Executing SQL took 0.00267 secs
#> Executing SQL took 0.00257 secs
#> Executing SQL took 0.00323 secs
#> Executing SQL took 0.00311 secs
#> Executing SQL took 0.00257 secs
#> Executing SQL took 0.00249 secs
#> Executing SQL took 0.0028 secs
#> Executing SQL took 0.00258 secs
#> Executing SQL took 0.00273 secs
#> Executing SQL took 0.00258 secs
#> Executing SQL took 0.00264 secs
#> Creating characterization results tables
#> Executing SQL took 0.0159 secs
#> Migrating data set
#> Migrator using SQL files in Characterization
#> Connecting using SQLite driver
#> Creating migrations table
#>   |                                                                              |                                                                      |   0%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0028 secs
#> Migrations table created
#> Executing migration: Migration_1-v3_0_0_store_version.sql
#>   |                                                                              |                                                                      |   0%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00309 secs
#> Saving migration: Migration_1-v3_0_0_store_version.sql
#>   |                                                                              |                                                                      |   0%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0026 secs
#> Migration complete Migration_1-v3_0_0_store_version.sql
#> Closing database connection
#> Updating version number
#> Executing SQL took 0.0033 secs

# insert results
insertResultsToDatabase(
 connectionDetails = charResultDbCD,
 schema = 'main',
 resultsFolder = file.path(tempdir(),'database'),
 includedFiles = c('time_to_event')
)
#> Connecting using SQLite driver
#> Uploading file: c_time_to_event.csv to table: c_time_to_event
#> - Preparing to upload rows 1 through 96
#> Inserting data took 0.0109 secs
#> Uploading data took 1.25 secs

```
