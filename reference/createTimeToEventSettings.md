# Create time to event study settings

Create time to event study settings

## Usage

``` r
createTimeToEventSettings(studyPopulationSettings, outcomeIds)
```

## Arguments

- studyPopulationSettings:

  An object created using `createStudyPopulationSettings` or a list of
  `createStudyPopulationSettings` that specifies cohort inclusion
  criteria

- outcomeIds:

  A list of cohortIds for the outcome cohorts

## Value

An list with the time to event settings

## Examples

``` r
# example code

tteSet <- createTimeToEventSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = c(1,2),
    limitToFirstInNDays = 0,
    minPriorObservation = 0
    ),
  outcomeIds = 3
)

```
