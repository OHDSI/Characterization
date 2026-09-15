# Create cohort incidence study settings

Create cohort incidence study settings

## Usage

``` r
createCohortIncidenceSettings(
  studyPopulationSettings,
  outcomeIds,
  outcomeWashoutDays = 0,
  riskWindowStart = 1,
  startAnchor = "cohort start",
  riskWindowEnd = 365,
  endAnchor = "cohort start",
  byAge = FALSE,
  byGender = FALSE,
  byYear = FALSE,
  ageBreaks = NULL,
  ageBreakList = NULL,
  startDate = "",
  endDate = ""
)
```

## Arguments

- studyPopulationSettings:

  An object created using `createStudyPopulationSettings` or a list of
  `createStudyPopulationSettings` that specifies cohort inclusion
  criteria

- outcomeIds:

  A vector of cohortIds for the outcome cohorts

- outcomeWashoutDays:

  A vector of integers specifying the washout days for the outcomeIds

- riskWindowStart:

  The start of the risk window (in days) relative to the
  \`startAnchor\`.

- startAnchor:

  The anchor point for the start of the risk window. Can be \`"cohort
  start"\` or \`"cohort end"\`.

- riskWindowEnd:

  The end of the risk window (in days) relative to the \`endAnchor\`.

- endAnchor:

  The anchor point for the end of the risk window. Can be \`"cohort
  start"\` or \`"cohort end"\`.

- byAge:

  Whether to stratify the incidence rates by age groups (specified via
  ageBreaks or ageBreakList)

- byGender:

  Whether to stratify the incidence rates by gender

- byYear:

  Whether to stratify the incidence rates by index year

- ageBreaks:

  a vector of integers indicating the age group bounds

- ageBreakList:

  a list of ageBreaks, used to specify multiple age break strata.

- startDate:

  a character vector representing a date in YYYY-MM-DD format

- endDate:

  a character vector representing a date in YYYY-MM-DD format

## Value

An list with the cohort incidence settings

## Examples

``` r
# example code

ciSet <- createCohortIncidenceSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = c(1,2),
    limitToFirstInNDays = 0,
    minPriorObservation = 0
    ),
  outcomeIds = 3
)

```
