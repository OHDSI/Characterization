# Create the settings for a large scale characterization study

This function creates a list of settings for different characterization
studies

## Usage

``` r
createCharacterizationSettings(
  timeToEventSettings = NULL,
  dechallengeRechallengeSettings = NULL,
  targetBaselineSettings = NULL,
  riskFactorSettings = NULL,
  caseSeriesSettings = NULL,
  restrictWashoutToObs = TRUE
)
```

## Arguments

- timeToEventSettings:

  A list of timeToEvent settings

- dechallengeRechallengeSettings:

  A list of dechallengeRechallenge settings

- targetBaselineSettings:

  A list of targetBaselineSettings settings

- riskFactorSettings:

  A list of riskFactorSettings settings

- caseSeriesSettings:

  A list of caseSeriesSettings settings

- restrictWashoutToObs:

  Whether to restrict to outcomes in observation period for washout in
  risk factors

## Value

Returns the connection to the sqlite database

## Details

Specify one or more timeToEvent, dechallengeRechallenge and
aggregateCovariate settings

## See also

Other LargeScale:
[`loadCharacterizationSettings()`](loadCharacterizationSettings.md),
[`runCharacterizationAnalyses()`](runCharacterizationAnalyses.md),
[`saveCharacterizationSettings()`](saveCharacterizationSettings.md)

## Examples

``` r
# example code

drSet <- createDechallengeRechallengeSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

cSet <- createCharacterizationSettings(
  dechallengeRechallengeSettings = drSet
)
```
