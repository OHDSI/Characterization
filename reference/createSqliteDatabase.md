# Create an sqlite database connection

This function creates a connection to an sqlite database

## Usage

``` r
createSqliteDatabase(sqliteLocation = tempdir())
```

## Arguments

- sqliteLocation:

  The location of the sqlite database

## Value

Returns the connection detail object to the sqlite database

## Details

This function creates a sqlite database and connection

## See also

Other Database:
[`createCharacterizationTables()`](createCharacterizationTables.md),
[`insertResultsToDatabase()`](insertResultsToDatabase.md)

## Examples

``` r
charResultDbCD <- createSqliteDatabase()

```
