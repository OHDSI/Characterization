dbmsPlatforms <- c("bigquery", "oracle", "postgresql", "redshift", "snowflake", "spark", "sql server")
connectionDetails <- Eunomia::getEunomiaConnectionDetails(databaseFile = "testEunomia.sqlite")
Eunomia::createCohorts(connectionDetails = connectionDetails)

withr::defer(
  {
    unlink("testEunomia.sqlite", recursive = TRUE, force = TRUE)
  },
  testthat::teardown_env()
)
