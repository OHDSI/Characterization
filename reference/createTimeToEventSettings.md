# Create time to event study settings

Create time to event study settings

## Usage

``` r
createTimeToEventSettings(targetIds, outcomeIds)
```

## Arguments

- targetIds:

  A list of cohortIds for the target cohorts

- outcomeIds:

  A list of cohortIds for the outcome cohorts

## Value

An list with the time to event settings

## See also

Other TimeToEvent:
[`computeTimeToEventAnalyses()`](computeTimeToEventAnalyses.md)

## Examples

``` r
# example code

tteSet <- createTimeToEventSettings(
  targetIds = c(1,2),
  outcomeIds = 3
)

```
