# Create dechallenge rechallenge study settings

Create dechallenge rechallenge study settings

## Usage

``` r
createDechallengeRechallengeSettings(
  studyPopulationSettings,
  outcomeIds,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 30
)
```

## Arguments

- studyPopulationSettings:

  An object created using `createStudyPopulationSettings` of a list of
  `createStudyPopulationSettings` that specifies cohort inclusion
  criteria

- outcomeIds:

  A list of cohortIds for the outcome cohorts

- dechallengeStopInterval:

  An integer specifying the how much time to add to the cohort_end when
  determining whether the event starts during cohort and ends after

- dechallengeEvaluationWindow:

  An integer specifying the period of time after the cohort_end when you
  cannot see an outcome for a dechallenge success

## Value

A list with the settings

## Examples

``` r
drSet <- createDechallengeRechallengeSettings(
  studyPopulationSettings = createStudyPopulationSettings(
    targetIds = c(1,2),
    limitToFirstInNDays = 0,
    minPriorObservation = 0
    ),
  outcomeIds = 3
)

```
