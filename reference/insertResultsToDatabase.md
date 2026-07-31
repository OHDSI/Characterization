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

## generate results into resultsFolder
#conDet <- exampleOmopConnectionDetails()

#tteSet <- createTimeToEventSettings(
#  studyPopulationSettings = createStudyPopulationSettings(
#    targetIds = c(1,2),
#    limitToFirstInNDays = 0,
#    minPriorObservation = 0
#    ),
#  outcomeIds = 3
#  )

#cSet <- createCharacterizationSettings(
#  timeToEventSettings = tteSet
#)

#runCharacterizationAnalyses(
#  connectionDetails = conDet,
#  targetDatabaseSchema = 'main',
#  targetTable = 'cohort',
#  outcomeDatabaseSchema = 'main',
#  outcomeTable = 'cohort',
#  cdmDatabaseSchema = 'main',
#  characterizationSettings = cSet,
#  outputDirectory = file.path(tempdir(),'database')
#)

## create sqlite database
#charResultDbCD <- createSqliteDatabase()

## create database results tables
#createCharacterizationTables(
#   connectionDetails = charResultDbCD,
#   resultSchema = 'main'
# )

## insert results
#insertResultsToDatabase(
# connectionDetails = charResultDbCD,
# schema = 'main',
# resultsFolder = file.path(tempdir(),'database'),
# includedFiles = c('time_to_event')
#)

```
