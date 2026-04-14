# viewCharacterization - Interactively view the characterization results

This is a shiny app for viewing interactive plots and tables

## Usage

``` r
viewCharacterization(resultFolder, cohortDefinitionSet = NULL)
```

## Arguments

- resultFolder:

  The location of the csv results

- cohortDefinitionSet:

  The cohortDefinitionSet extracted using webAPI

## Value

Opens a shiny app for interactively viewing the results

## Details

Input is the output of ...

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
  outputDirectory = file.path(tempdir(),'view')
)
#> Creating directory /tmp/Rtmp2RLAmD/view
#> Creating directory /tmp/Rtmp2RLAmD/view/execution
#> Connecting using SQLite driver
#> Extracting cohort jobs
#> Connecting using SQLite driver
#> Creating new cluster
#> Connecting using SQLite driver
#> Uploading #cohort_settings
#> Inserting data took 0.00778 secs
#> Computing time to event results
#> Executing SQL took 0.0227 secs
#> Computing time-to-event for 2 T-O pairs took 0.217 secs
#> exporting to andromeda
#> Disconnected Andromeda. This data object can no longer be used
#> Removing any existing results in outputFolder directory
#> Loading andromeda result at /tmp/Rtmp2RLAmD/view/execution/tte_1

# interactive shiny app
if (FALSE) { # \dontrun{
viewCharacterization(
  resultFolder = file.path(tempdir(),'view')
)
} # }

```
