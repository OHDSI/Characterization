# Removes csv files from folders that have not been marked as completed and removes the record of the execution file

Removes csv files from folders that have not been marked as completed
and removes the record of the execution file

## Usage

``` r
cleanIncremental(executionFolder, ignoreWhenEmpty = FALSE)
```

## Arguments

- executionFolder:

  The folder that has the execution files

- ignoreWhenEmpty:

  When TRUE, if there are no incremental logs then nothing is run

## Value

A list with the settings

## See also

Other Incremental: [`cleanNonIncremental()`](cleanNonIncremental.md)

## Examples

``` r
cleanIncremental(
  file.path(tempdir(), 'incremental'),
  ignoreWhenEmpty = TRUE
)

```
