# Create the results tables to store characterization results into a database

This function executes a large set of SQL statements to create tables
that can store results

## Usage

``` r
createCharacterizationTables(
  connectionDetails,
  resultSchema,
  targetDialect = "postgresql",
  deleteExistingTables = TRUE,
  createTables = TRUE,
  tablePrefix = "c_",
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
)
```

## Arguments

- connectionDetails:

  The connectionDetails to a database created by using the function
  `createConnectDetails` in the `DatabaseConnector` package.

- resultSchema:

  The name of the database schema that the result tables will be
  created.

- targetDialect:

  The database management system being used

- deleteExistingTables:

  If true any existing tables matching the Characterization result
  tables names will be deleted

- createTables:

  If true the Characterization result tables will be created

- tablePrefix:

  A string appended to the Characterization result tables

- tempEmulationSchema:

  The temp schema used when the database management system is oracle

## Value

Returns NULL but creates the required tables into the specified database
schema.

## Details

This function can be used to create (or delete) Characterization result
tables

## See also

Other Database: [`createSqliteDatabase()`](createSqliteDatabase.md),
[`insertResultsToDatabase()`](insertResultsToDatabase.md)

## Examples

``` r
# create sqlite database
charResultDbCD <- createSqliteDatabase()

# create database results tables
createCharacterizationTables(
   connectionDetails = charResultDbCD,
   resultSchema = 'main'
 )
#> Connecting using SQLite driver
#> Deleting existing tables
#> Creating characterization results tables
#> Executing SQL took 0.012 secs
#> Migrating data set
#> Migrator using SQL files in Characterization
#> Connecting using SQLite driver
#> Creating migrations table
#>   |                                                                              |                                                                      |   0%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00352 secs
#> Migrations table created
#> Executing migration: Migration_1-v3_0_0_store_version.sql
#>   |                                                                              |                                                                      |   0%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00337 secs
#> Saving migration: Migration_1-v3_0_0_store_version.sql
#>   |                                                                              |                                                                      |   0%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0029 secs
#> Migration complete Migration_1-v3_0_0_store_version.sql
#> Executing migration: Migration_2-v4_0_0_table_change.sql
#>   |                                                                              |                                                                      |   0%  |                                                                              |==                                                                    |   4%  |                                                                              |=====                                                                 |   7%  |                                                                              |========                                                              |  11%  |                                                                              |==========                                                            |  14%  |                                                                              |============                                                          |  18%  |                                                                              |===============                                                       |  21%  |                                                                              |==================                                                    |  25%  |                                                                              |====================                                                  |  29%  |                                                                              |======================                                                |  32%  |                                                                              |=========================                                             |  36%  |                                                                              |============================                                          |  39%  |                                                                              |==============================                                        |  43%  |                                                                              |================================                                      |  46%  |                                                                              |===================================                                   |  50%  |                                                                              |======================================                                |  54%  |                                                                              |========================================                              |  57%  |                                                                              |==========================================                            |  61%  |                                                                              |=============================================                         |  64%  |                                                                              |================================================                      |  68%  |                                                                              |==================================================                    |  71%  |                                                                              |====================================================                  |  75%  |                                                                              |=======================================================               |  79%  |                                                                              |==========================================================            |  82%  |                                                                              |============================================================          |  86%  |                                                                              |==============================================================        |  89%  |                                                                              |=================================================================     |  93%  |                                                                              |====================================================================  |  96%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.0242 secs
#> Saving migration: Migration_2-v4_0_0_table_change.sql
#>   |                                                                              |                                                                      |   0%  |                                                                              |======================================================================| 100%
#> Executing SQL took 0.00474 secs
#> Migration complete Migration_2-v4_0_0_table_change.sql
#> Closing database connection
#> Updating version number to 4.0.1
#> Executing SQL took 0.00337 secs
```
