# specify databases to test

dbmsPlatforms <- c(
  "bigquery",
  "oracle",
  "postgresql",
  "redshift",
  "snowflake",
  "spark",
  "sql server"
)

getPlatformConnectionDetails <- function(dbmsPlatform) {
  # Get drivers for test platform
  if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
    jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  } else {
    jdbcDriverFolder <- "~/.jdbcDrivers"
    dir.create(jdbcDriverFolder, showWarnings = FALSE)
  }

  options("sqlRenderTempEmulationSchema" = NULL)
  if (dbmsPlatform == "sqlite") {
    connectionDetails <- Eunomia::getEunomiaConnectionDetails()
    cdmDatabaseSchema <- "main"
    vocabularyDatabaseSchema <- "main"
    cohortDatabaseSchema <- "main"
    options("sqlRenderTempEmulationSchema" = NULL)
    cohortTable <- "cohort"
  } else {
    if (dbmsPlatform == "bigquery") {
      # To avoid rate limit on BigQuery, only test on 1 OS:
      if (.Platform$OS.type == "windows") {
        bqKeyFile <- tempfile(fileext = ".json")
        writeLines(Sys.getenv("CDM_BIG_QUERY_KEY_FILE"), bqKeyFile)
        if (testthat::is_testing()) {
          withr::defer(unlink(bqKeyFile, force = TRUE), testthat::teardown_env())
        }
        bqConnectionString <- gsub(
          "<keyfile path>",
          normalizePath(bqKeyFile, winslash = "/"),
          Sys.getenv("CDM_BIG_QUERY_CONNECTION_STRING")
        )
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = dbmsPlatform,
          user = "",
          password = "",
          connectionString = !!bqConnectionString,
          pathToDriver = jdbcDriverFolder
        )
        cdmDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
        vocabularyDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_CDM_SCHEMA")
        cohortDatabaseSchema <- Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA")
        options(sqlRenderTempEmulationSchema = Sys.getenv("CDM_BIG_QUERY_OHDSI_SCHEMA"))
      } else {
        return(NULL)
      }
    } else if (dbmsPlatform == "oracle") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_ORACLE_USER"),
        password = URLdecode(Sys.getenv("CDM5_ORACLE_PASSWORD")),
        server = Sys.getenv("CDM5_ORACLE_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_ORACLE_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_ORACLE_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "postgresql") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
        password = URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
        server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")
    } else if (dbmsPlatform == "redshift") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_REDSHIFT_USER"),
        password = URLdecode(Sys.getenv("CDM5_REDSHIFT_PASSWORD")),
        server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_REDSHIFT_OHDSI_SCHEMA")
    } else if (dbmsPlatform == "snowflake") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM_SNOWFLAKE_USER"),
        password = URLdecode(Sys.getenv("CDM_SNOWFLAKE_PASSWORD")),
        connectionString = Sys.getenv("CDM_SNOWFLAKE_CONNECTION_STRING"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_CDM53_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA")
      options(sqlRenderTempEmulationSchema = Sys.getenv("CDM_SNOWFLAKE_OHDSI_SCHEMA"))
    } else if (dbmsPlatform == "spark") {
      if (.Platform$OS.type == "windows") { # skipping Mac for GHA due to JAVA issue
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = dbmsPlatform,
          user = Sys.getenv("CDM5_SPARK_USER"),
          password = URLdecode(Sys.getenv("CDM5_SPARK_PASSWORD")),
          connectionString = Sys.getenv("CDM5_SPARK_CONNECTION_STRING"),
          pathToDriver = jdbcDriverFolder
        )
        cdmDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
        vocabularyDatabaseSchema <- Sys.getenv("CDM5_SPARK_CDM_SCHEMA")
        cohortDatabaseSchema <- Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA")
        options(sqlRenderTempEmulationSchema = Sys.getenv("CDM5_SPARK_OHDSI_SCHEMA"))
      } else {
        return(NULL)
      }
    } else if (dbmsPlatform == "sql server") {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = dbmsPlatform,
        user = Sys.getenv("CDM5_SQL_SERVER_USER"),
        password = URLdecode(Sys.getenv("CDM5_SQL_SERVER_PASSWORD")),
        server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
        pathToDriver = jdbcDriverFolder
      )
      cdmDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      vocabularyDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA")
      cohortDatabaseSchema <- Sys.getenv("CDM5_SQL_SERVER_OHDSI_SCHEMA")
    }

    # Add drivers
    DatabaseConnector::downloadJdbcDrivers(dbmsPlatform, pathToDriver = jdbcDriverFolder)
    # Table created to avoid collisions
    cohortTable <- paste0("ct_", Sys.getpid(), format(Sys.time(), "%s"), sample(1:100, 1))
  }

  return(list(
    dbmsPlatform = dbmsPlatform,
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema
  ))
}

for (dbmsPlatform in dbmsPlatforms) {
  if (Sys.getenv("CI") == "true" & .Platform$OS.type == "windows") {
    tempFolder <- tempfile(paste0("Characterization_", dbmsPlatform))
    on.exit(unlink(tempFolder, recursive = TRUE), add = TRUE)

    dbmsDetails <- getPlatformConnectionDetails(dbmsPlatform)
    if (!is.null(dbmsDetails)) {
      con <- DatabaseConnector::connect(dbmsDetails$connectionDetails)
      on.exit(DatabaseConnector::disconnect(con))
    }
  }

  # This file contains platform specific tests
  test_that(paste0("platform specific test ", dbmsPlatform), {
    skip_if(Sys.getenv("CI") != "true" | .Platform$OS.type != "windows", "not run locally")
    if (is.null(dbmsDetails)) {
      print(paste("No platform details available for", dbmsPlatform))
    } else {
      # create a cohort table
      DatabaseConnector::insertTable(
        bulkLoad = F,
        connection = con,
        databaseSchema = dbmsDetails$cohortDatabaseSchema,
        tableName = dbmsDetails$cohortTable,
        data = data.frame(
          subject_id = 1:10,
          cohort_definition_id = sample(4, 10, replace = T),
          cohort_start_date = rep(as.Date("2010-01-01"), 10),
          cohort_end_date = rep(as.Date("2010-01-01"), 10)
        )
      )

      targetIds <- c(1, 2, 4)
      outcomeIds <- c(3)

      timeToEventSettings1 <- createTimeToEventSettings(
        targetIds = 1,
        outcomeIds = c(3, 4)
      )
      timeToEventSettings2 <- createTimeToEventSettings(
        targetIds = 2,
        outcomeIds = c(3, 4)
      )

      dechallengeRechallengeSettings <- createDechallengeRechallengeSettings(
        targetIds = targetIds,
        outcomeIds = outcomeIds,
        dechallengeStopInterval = 30,
        dechallengeEvaluationWindow = 31
      )

      aggregateCovariateSettings1 <- createAggregateCovariateSettings(
        targetIds = targetIds,
        outcomeIds = outcomeIds,
        riskWindowStart = 1,
        startAnchor = "cohort start",
        riskWindowEnd = 365,
        endAnchor = "cohort start",
        covariateSettings = FeatureExtraction::createCovariateSettings(
          useDemographicsGender = T,
          useDemographicsAge = T,
          useDemographicsRace = T
        )
      )

      aggregateCovariateSettings2 <- createAggregateCovariateSettings(
        targetIds = targetIds,
        outcomeIds = outcomeIds,
        riskWindowStart = 1,
        startAnchor = "cohort start",
        riskWindowEnd = 365,
        endAnchor = "cohort start",
        covariateSettings = FeatureExtraction::createCovariateSettings(
          useConditionOccurrenceLongTerm = T
        )
      )

      characterizationSettings <- createCharacterizationSettings(
        timeToEventSettings = list(
          timeToEventSettings1,
          timeToEventSettings2
        ),
        dechallengeRechallengeSettings = list(
          dechallengeRechallengeSettings
        ),
        aggregateCovariateSettings = list(
          aggregateCovariateSettings1,
          aggregateCovariateSettings2
        )
      )

      runCharacterizationAnalyses(
        connectionDetails = dbmsDetails$connectionDetails,
        cdmDatabaseSchema = dbmsDetails$cdmDatabaseSchema,
        targetDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
        targetTable = dbmsDetails$cohortTable,
        outcomeDatabaseSchema = dbmsDetails$cohortDatabaseSchema,
        outcomeTable = dbmsDetails$cohortTable,
        characterizationSettings = characterizationSettings,
        outputDirectory = file.path(tempFolder, "csv"),
        executionPath = file.path(tempFolder, "execution"),
        csvFilePrefix = "c_",
        threads = 1,
        databaseId = dbmsDetails$connectionDetails$dbms
      )

      testthat::expect_true(
        length(dir(file.path(tempFolder, "csv"))) > 0
      )

      # check cohort details is saved
      testthat::expect_true(
        file.exists(file.path(tempFolder, "csv", "c_cohort_details.csv"))
      )
      testthat::expect_true(
        file.exists(file.path(tempFolder, "csv", "c_settings.csv"))
      )
      testthat::expect_true(
        file.exists(file.path(tempFolder, "csv", "c_analysis_ref.csv"))
      )
      testthat::expect_true(
        file.exists(file.path(tempFolder, "csv", "c_covariate_ref.csv"))
      )
    }
  })
}
