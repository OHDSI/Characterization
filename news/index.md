# Changelog

## Characterization 4.0.1

- added check to createCharacterizationSettings() to error if no
  settings were entered
- updated test file cleanup

## Characterization 4.0.0

- \[enhancement\] replaced targetId inputs to the settings with
  studyPopulationSettings that lets you specify min prior observation,
  first in n days, age, date, gender and nesting cohort restrictions.
- \[enhancement\] outcome washout in risk factor is now used to combine
  outcome cohort entries that are within outcome washout days
- \[enhancement\] improved attrition capture: can now see fill attrition
  for study population
- \[enhancement\] changed csv file output to split up attrition into
  case_attrition and target_attrition, added case_counts and
  target_counts and added
  time_to_event_settings/dechallenge_rechallenge_settings that let user
  quickly see what study population and outcome pairs were included.
- \[bug fix\] fixed issues when dividing by zero in SMD calculation

## Characterization 3.0.2

- Replacing IFNULL with ISNULL as SQL server errors with IFNULL

## Characterization 3.0.1

CRAN release: 2026-05-15

- Fix issue with uploading results into database for shiny viewer
  (spacing was added to csv and causing issues and continuous covariates
  that are floats were incorrectly bigints)

## Characterization 3.0.0

CRAN release: 2026-03-25

- Splitting the aggregateCovariates into: riskFactor, targetBaseline and
  caseSeries to make the inputs clearer.
- Adding a new function for cohort generation for the riskFactor,
  targetBaseline and caseSeries settings to ensure cohort generation is
  efficient and remove redundancies.
- Updating the results tables for the partitioning of
  aggregateCovariates into riskFactor, targetBaseline and caseSeries
- riskFactor analysis now has three modes: efficient (target without
  case vs case), CohortIncidence (target without case who have some TAR
  vs case) and PatientLevelPrediction (target without case and without
  people with outcome during washout period before TAR vs case).
- riskFactor analysis calculates SMD during execution and min SMD values
  can be specified to reduce result size.  
- User can specify the number of target jobs (how many groups of
  targetIds to partition the jobs into) and the number of threads (how
  many threads to run the jobs in parallel).
- Intermediate results are stored as Andromeda files rather than csv
  files
- minCellCount censoring is done when exporting the intermediate results
  so it can be changed after execution without rerunning whole analysis.
- When running in incremental model results are checked to ensure all
  jobs executed successful before exporting to csv otherwise an error is
  returned to show some jobs were incomplete.
- riskFactor and caseSeries use the new FeatureExtarction option of
  exporting the features into a database table rather than downloading
  this enables additional processing to be done efficiently in SQL
  instead of R.
- The cohorts used by riskFactor, targetBaseline and caseSeries are
  saved into a user specified database and table
- The attritions for riskFactor, targetBaseline and caseSeries cohort
  creation are saved to enable users to see where patients were removed.

## Characterization 2.2.0

CRAN release: 2025-09-10

- fixed csv spec: made mean_exposure_time a float and specified that
  min_characterization_mean in covariate table must be non-null and is
  in the pk.
- changed Line 284 in AggregateCovariates.R to cast exposure_time
  summary values to bigint due to integer overflow in some dbms.
- added dummy sql code to prevent warnings about missing variables
- added code to save empty csv files when there are no rows as that way
  it is easier to see there are no results vs an error saving.
- removed progress bar from custom during features
- added option includedFiles in insertResultsToDatabase() where you can
  specify the csv files to upload to prevent warnings of missing csv
  files.
- made sure all connections are disconnected after use
- fixed counts to use count_big (thanks Anthony Sena) to fix an issue
  where the number was bigger than an integer.
- added code to copy csv files in batches this is needed when the csv
  files are very large.

## Characterization 2.1.3

CRAN release: 2025-03-04

- prepared for CRAN by adding examples, removing getwd(), replacing T/F
  with TRUE/FALSE and added example data inside package so no download
  required.

## Characterization 2.1.2

- added input ignoreWhenEmpty to cleanIncremental() that does not run
  incrementalClean if there are no incremental files

## Characterization 2.1.1

- fixed result database column type for mean_exposure_time

## Characterization 2.1.0

- risk factors and case series now restrict to first outcome only.
- added documentation describing the different analyses with examples.

## Characterization 2.0.1

- edited cohort_type in results to varchar(12)
- fixed setting id being messed up by readr loading

## Characterization 2.0.0

- added tests for all HADES supported dbms
- updated minCellCount censoring
- fixed issues with incremental
- made the code more modular to enable new characterizations to be added
- added job optimization code to optimize the distributuion of jobs
- fixed tests and made minor bug updates

## Characterization 1.0.0

- Added parallelization in the aggregate covariates analysis
- Extact all results as csv files
- Removed sqlite result creation
- now using ResultModelManager to upload results into database

## Characterization 0.3.1

- Removed DatabaseConnector from Remotes in DESCRIPTION. Fixes GitHb
  issue 38.
- Added check to covariateSettings input in
  createAggregateCovariateSettings to error if temporal is T
- adding during cohort covariate settings
- added a case covariate settings inputs to aggregate covariates
- added casePreTargetDuration and casePostTreatmentDuration integer
  inputs to aggregate covariates

## Characterization 0.3.0

- Added new outcomeWashoutDays parameter to
  createAggregateCovariateSettings to remove outcome occurances that are
  continuations of a prior outcome occurrence
- Changed the way cohort definition ids are created in
  createAggregateCovariateSettings to use hash of target id, outcome id
  and type. This lets users combine different studies into a single
  result database.
- Added database migration capability and created new migrations for the
  recent updates.

## Characterization 0.2.0

Updated dependency to FeatureExtraction (\>= 3.5.0) to support
minCharacterizationMean paramater.

## Characterization 0.1.5

Changed export to csv approach to use batch export from SQLite
([\#41](https://github.com/OHDSI/Characterization/issues/41))

## Characterization 0.1.4

Added extra error logging

## Characterization 0.1.3

Optimized aggregate features to remove T and not Os (as these can be
calculated using T and T and Os) - requires latest shiny app though
Optimized database extraction to csv

## Characterization 0.1.2

Fixing bug where first outcome was still all outcomes Updating shiny app
to work with old and new ShinyAppBuilder

## Characterization 0.1.1

Fixing bug where cohort_counts were not being saved in the database

## Characterization 0.1.0

- added support to enable target cohorts with multiple cohort entries
  for the aggregate covariate analysis by restricting to first cohort
  entry and ensuring the subject has a user specified
  minPriorObservation days observation in the database at first entry
  and also perform analysis on first outcomes and any outcome that is
  recorded during TAR.
- added shiny app

## Characterization 0.0.1

Initial version
