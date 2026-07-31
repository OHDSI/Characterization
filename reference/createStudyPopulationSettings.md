# create the study population settings

create the study population settings

## Usage

``` r
createStudyPopulationSettings(
  targetIds,
  limitToFirstInNDays = 0,
  minPriorObservation = 0,
  nestingCohortId = NULL,
  minAge = NULL,
  maxAge = NULL,
  studyStartDate = NULL,
  studyEndDate = NULL,
  genderConceptIds = NULL
)
```

## Arguments

- targetIds:

  A target cohort id or vector of target cohort ids to do the subsetting
  to

- limitToFirstInNDays:

  Should only the first exposure in N days per subject be included?

- minPriorObservation:

  The minimum required continuous observation time prior to index date
  for a person to be included in the cohort.

- nestingCohortId:

  A cohort definition id to restrict the target cohort. Patient in the
  target cohort are only included if they are also in the nesting cohort
  at index.

- minAge:

  The minimum age required to be in the target at index

- maxAge:

  The maximum age required to be in the target at index

- studyStartDate:

  The earliest date to be included into the target. Date format is
  'yyyymmdd'.

- studyEndDate:

  The latest date to be included into the target. Date format is
  'yyyymmdd'.

- genderConceptIds:

  A target cohort subject's gender concept to restrict to

## Value

A data.frame containing all the settings required for creating the study
populations of interest

## See also

Other helper:
[`exampleOmopConnectionDetails()`](exampleOmopConnectionDetails.md)

## Examples

``` r
# Create study population settings with a washout period of 365 days and
# restricted to adults for target dates that occur for the first time in 365 days.
populationSettings <- createStudyPopulationSettings(
   targetId  = 1,
   limitToFirstInNDays = 365,
   minPriorObservation = 365,
   minAge = 18
   )
```
