# Package index

## Aggregate Covariate Analysis

This analysis calculates the aggregate characteristics for a Target
cohort (T), an Outcome cohort (O) and combiations of T with O during
time at risk and T without O during time at risk.

- [`createCaseSeriesSettings()`](createCaseSeriesSettings.md) : Create
  aggregate covariate study settings
- [`createRiskFactorSettings()`](createRiskFactorSettings.md) : Create
  risk factor study settings
- [`createTargetBaselineSettings()`](createTargetBaselineSettings.md) :
  Create target baseline aggregate covariate study settings

## Dechallenge Rechallenge Analysis

For a given Target cohort (T) and Outcome cohort (O) find any
occurrances of a dechallenge (when the T cohort stops close to when O
started) and a rechallenge (when T restarts and O starts again) This is
useful for investigating causality between drugs and events.

- [`createDechallengeRechallengeSettings()`](createDechallengeRechallengeSettings.md)
  : Create dechallenge rechallenge study settings

## Time to Event Analysis

This analysis calculates the timing between the Target cohort (T) and an
Outcome cohort (O).

- [`createTimeToEventSettings()`](createTimeToEventSettings.md) : Create
  time to event study settings

## Run Large Scale Characterization Study

Run multipe aggregate covariate analysis, time to event and
dechallenge/rechallenge studies.

- [`createCharacterizationSettings()`](createCharacterizationSettings.md)
  : Create the settings for a large scale characterization study
- [`loadCharacterizationSettings()`](loadCharacterizationSettings.md) :
  Load the characterization settings previously saved as a json file
- [`runCharacterizationAnalyses()`](runCharacterizationAnalyses.md) :
  execute a large-scale characterization study
- [`saveCharacterizationSettings()`](saveCharacterizationSettings.md) :
  Save the characterization settings as a json

## Save Load

Functions to save the analysis settings and the results (as sqlite or
csv files).

## Insert into Database

Functions to insert the results into a database.

- [`createCharacterizationTables()`](createCharacterizationTables.md) :
  Create the results tables to store characterization results into a
  database
- [`createSqliteDatabase()`](createSqliteDatabase.md) : Create an sqlite
  database connection
- [`insertResultsToDatabase()`](insertResultsToDatabase.md) : Upload the
  results into a result database

## Shiny App

Functions to interactively exlore the results from
runCharacterizationAnalyses().

- [`viewCharacterization()`](viewCharacterization.md) :
  viewCharacterization - Interactively view the characterization results

## Custom covariates

Code to create covariates during cohort start and end

- [`createDuringCovariateSettings()`](createDuringCovariateSettings.md)
  : Create during covariate settings
- [`getDbDuringCovariateData()`](getDbDuringCovariateData.md) : Extracts
  covariates that occur during a cohort

## Incremental

Code to run incremetal model

- [`cleanIncremental()`](cleanIncremental.md) : Removes csv files from
  folders that have not been marked as completed and removes the record
  of the execution file
- [`cleanNonIncremental()`](cleanNonIncremental.md) : Removes csv files
  from the execution folder as there should be no csv files when running
  in non-incremental model

## Helpers

Helper functions such as example data for users

- [`createStudyPopulationSettings()`](createStudyPopulationSettings.md)
  : create the study population settings
- [`exampleOmopConnectionDetails()`](exampleOmopConnectionDetails.md) :
  create a connection detail for an example GI Bleed dataset from
  Eunomia
