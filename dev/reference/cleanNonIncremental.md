# Removes csv files from the execution folder as there should be no csv files when running in non-incremental model

Removes csv files from the execution folder as there should be no csv
files when running in non-incremental model

## Usage

``` r
cleanNonIncremental(executionFolder)
```

## Arguments

- executionFolder:

  The folder that has the execution files

## Value

A list with the settings

## See also

Other Incremental: [`cleanIncremental()`](cleanIncremental.md)

## Examples

``` r
# example code

cleanNonIncremental(file.path(tempdir(), 'incremental'))
```
