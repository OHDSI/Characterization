# Load the characterization settings previously saved as a json file

This function converts the json file back into an R object

## Usage

``` r
loadCharacterizationSettings(fileName)
```

## Arguments

- fileName:

  The location of the the json settings

## Value

Returns the json settings as an R object

## Details

Input the directory containing the 'characterizationSettings.json' file
and load the settings into R

## See also

Other LargeScale:
[`createCharacterizationSettings()`](createCharacterizationSettings.md),
[`runCharacterizationAnalyses()`](runCharacterizationAnalyses.md),
[`saveCharacterizationSettings()`](saveCharacterizationSettings.md)

## Examples

``` r
# example code

setPath <- file.path(tempdir(), 'charSet.json')

drSet <- createDechallengeRechallengeSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

cSet <- createCharacterizationSettings(
  dechallengeRechallengeSettings = drSet
)

saveCharacterizationSettings(
  settings = cSet,
  fileName = setPath
)

setting <- loadCharacterizationSettings(setPath)

```
