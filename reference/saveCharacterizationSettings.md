# Save the characterization settings as a json

This function converts the settings into a json object and saves it

## Usage

``` r
saveCharacterizationSettings(settings, fileName)
```

## Arguments

- settings:

  An object of class characterizationSettings created using
  `createCharacterizationSettings`

- fileName:

  The location to save the json settings

## Value

Returns the location of the directory containing the json settings

## Details

Input the characterization settings and output a json file to a file
named 'characterizationSettings.json' inside the saveDirectory

## See also

Other LargeScale:
[`createCharacterizationSettings()`](createCharacterizationSettings.md),
[`loadCharacterizationSettings()`](loadCharacterizationSettings.md),
[`runCharacterizationAnalyses()`](runCharacterizationAnalyses.md)

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

cSet <- createCharacterizationSettings(
  dechallengeRechallengeSettings = drSet
)

saveCharacterizationSettings(
  settings = cSet,
  fileName = file.path(tempdir(), 'cSet.json')
)
```
