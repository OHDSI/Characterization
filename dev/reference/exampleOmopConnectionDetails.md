# create a connection detail for an example GI Bleed dataset from Eunomia

This returns an object of class \`ConnectionDetails\` that lets you
connect via \`DatabaseConnector::connect()\` to the example database.

## Usage

``` r
exampleOmopConnectionDetails(exdir = tempdir())
```

## Arguments

- exdir:

  a directory to unzip the example OMOP database into. Default is
  tempdir().

## Value

An object of class \`ConnectionDetails\` with the details to connect to
the example OHDSI OMOP CDM database

## Details

Finds the location of the example database in the package and calls
\`DatabaseConnector::createConnectionDetails\` to create a
\`ConnectionDetails\` object for connecting to the database.

## Examples

``` r
conDet <- exampleOmopConnectionDetails()

connectionHandler <- ResultModelManager::ConnectionHandler$new(conDet)
#> Connecting using SQLite driver
```
