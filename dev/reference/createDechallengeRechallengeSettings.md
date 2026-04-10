# Create dechallenge rechallenge study settings

Create dechallenge rechallenge study settings

## Usage

``` r
createDechallengeRechallengeSettings(
  targetIds,
  outcomeIds,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 30
)
```

## Arguments

- targetIds:

  A list of cohortIds for the target cohorts

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

## See also

Other DechallengeRechallenge:
[`computeDechallengeRechallengeAnalyses()`](computeDechallengeRechallengeAnalyses.md),
[`computeRechallengeFailCaseSeriesAnalyses()`](computeRechallengeFailCaseSeriesAnalyses.md)

## Examples

``` r
drSet <- createDechallengeRechallengeSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

```
