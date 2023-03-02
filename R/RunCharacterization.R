#' Create the settings for a large scale characterization study
#' @description
#' This function creates a list of settings for different characterization studies
#'
#' @details
#' Specify one or more timeToEvent, dechallengeRechallenge and aggregateCovariate settings
#'
#' @param timeToEventSettings    A list of timeToEvent settings
#' @param dechallengeRechallengeSettings A list of dechallengeRechallenge settings
#' @param aggregateCovariateSettings A list of aggregateCovariate settings
#'
#' @return
#' Returns the connection to the sqlite database
#'
#' @export
createCharacterizationSettings <- function(
    timeToEventSettings = NULL,
    dechallengeRechallengeSettings = NULL,
    aggregateCovariateSettings = NULL
)
{
  errorMessages <- checkmate::makeAssertCollection()
  .checkTimeToEventSettingsList(
    settings = timeToEventSettings,
    errorMessages =  errorMessages
  )
  .checkAggregateCovariateSettingsList(
    settings = aggregateCovariateSettings,
    errorMessages = errorMessages
  )
  .checkDechallengeRechallengeSettingsList(
    settings = dechallengeRechallengeSettings,
    errorMessages = errorMessages
  )

  if (class(timeToEventSettings) == "timeToEventSettings") {
    timeToEventSettings <- list(timeToEventSettings)
  }
  if (class(dechallengeRechallengeSettings) == "dechallengeRechallengeSettings") {
    dechallengeRechallengeSettings <- list(dechallengeRechallengeSettings)
  }
  if (class(aggregateCovariateSettings) == "aggregateCovariateSettings") {
    aggregateCovariateSettings <- list(aggregateCovariateSettings)
  }

  valid <- checkmate::reportAssertions(errorMessages)

  settings <- list(
    timeToEventSettings = timeToEventSettings,
    dechallengeRechallengeSettings = dechallengeRechallengeSettings,
    aggregateCovariateSettings = aggregateCovariateSettings
  )

  class(settings) <- "characterizationSettings"

  return(settings)
}

#' Save the characterization settings as a json
#' @description
#' This function converts the settings into a json object and saves it
#'
#' @details
#' Input the characterization settings and output a json file to a file named 'characterizationSettings.json' inside the saveDirectory
#'
#' @param settings    An object of class characterizationSettings created using \code{createCharacterizationSettings}
#' @param fileName  The location to save the json settings
#'
#' @return
#' Returns the location of the drectory containing the json settings
#'
#' @export
saveCharacterizationSettings <- function(
    settings,
    fileName
) {
  ParallelLogger::saveSettingsToJson(
    object = settings,
    fileName = fileName
  )

  invisible(fileName)
}

#' Load the characterization settings previously saved as a json file
#' @description
#' This function converts the json file back into an R object
#'
#' @details
#' Input the directory containing the 'characterizationSettings.json' file and load the settings into R
#'
#' @param fileName  The location of the the json settings
#'
#' @return
#' Returns the json settings as an R object
#'
#' @export
loadCharacterizationSettings <- function(
    fileName
) {

  settings <- ParallelLogger::loadSettingsFromJson(
    fileName = fileName
  )

  return(settings)
}

#' execute a large-scale characterization study
#' @description
#' Specify the database connection containing the CDM data, the cohort database schemas/tables,
#' the characterization settings and the directory to save the results to
#'
#' @details
#' The results of the characterization will be saved into an sqlite database inside the
#' specified saveDirectory
#'
#' @param connectionDetails  The connection details to the database containing the OMOP CDM data
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param cdmDatabaseSchema The schema with the OMOP CDM data
#' @param characterizationSettings The study settings created using \code{createCharacterizationSettings}
#' @param saveDirectory The location to save the results to
#' @param tablePrefix A string to append the tables in the results
#' @param databaseId The unqiue identifier for the cdm database
#' @param showSubjectId  Whether to include subjectId of failed rechallenge case series or hide
#'
#' @return
#' An sqlite database with the results is saved into the saveDirectory and a csv file named tacker.csv
#' details which analyses have run to completion.
#'
#' @export
runCharacterizationAnalyses <- function(
    connectionDetails,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    tempEmulationSchema = NULL,
    cdmDatabaseSchema,
    characterizationSettings,
    saveDirectory,
    tablePrefix = "c_",
    databaseId = "1",
    showSubjectId = F
) {
  # inputs checks
  errorMessages <- checkmate::makeAssertCollection()
  .checkCharacterizationSettings(
    settings = characterizationSettings,
    errorMessages = errorMessages
  )
  .checkTablePrefix(
    tablePrefix = tablePrefix,
    errorMessages = errorMessages
    )
  checkmate::reportAssertions(
    errorMessages
    )

  # create the Database
  conn <- createSqliteDatabase(
    sqliteLocation = saveDirectory
  )
  on.exit(
    DatabaseConnector::disconnect(conn)
    )

  createCharacterizationTables(
    conn = conn,
    resultSchema = "main",
    targetDialect = "sqlite",
    deleteExistingTables = T,
    createTables = T,
    tablePrefix = tablePrefix
  )

  if (!is.null(characterizationSettings$timeToEventSettings)) {
    for (i in 1:length(characterizationSettings$timeToEventSettings)) {

      message("Running time to event analysis ", i)

      result <- tryCatch(
        {
          computeTimeToEventAnalyses(
            connectionDetails = connectionDetails,
            targetDatabaseSchema = targetDatabaseSchema,
            targetTable = targetTable,
            outcomeDatabaseSchema = outcomeDatabaseSchema,
            outcomeTable = outcomeTable,
            tempEmulationSchema = tempEmulationSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            timeToEventSettings = characterizationSettings$timeToEventSettings[[i]],
            databaseId = databaseId
          )
        },
        error = function(e) {
          message(e);
          return(NULL)
        }
      )

      if (!is.null(result)) {
        # log that run was sucessful
        readr::write_csv(
          x = data.frame(
            analysis_type = "timeToEvent",
            run_id = i,
            database_id = databaseId,
            date_time = as.character(Sys.time())
          ),
          file = file.path(saveDirectory, "tracker.csv"),
          append = file.exists(file.path(saveDirectory, "tracker.csv"))
        )

        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "time_to_event",
          andromedaObject = result$timeToEvent,
          tablePrefix = tablePrefix
        )
      }

    }
  }

  if (!is.null(characterizationSettings$dechallengeRechallengeSettings)) {
    for (i in 1:length(characterizationSettings$dechallengeRechallengeSettings)) {
      ParallelLogger::logInfo(paste0("Running dechallenge rechallenge analysis ", i))

      result <- tryCatch(
        {
          computeDechallengeRechallengeAnalyses(
            connectionDetails = connectionDetails,
            targetDatabaseSchema = targetDatabaseSchema,
            targetTable = targetTable,
            outcomeDatabaseSchema = outcomeDatabaseSchema,
            outcomeTable = outcomeTable,
            tempEmulationSchema = tempEmulationSchema,
            dechallengeRechallengeSettings = characterizationSettings$dechallengeRechallengeSettings[[i]],
            databaseId = databaseId
          )
        },
        error = function(e) {
          message(e);
          return(NULL)
        }
      )

      if (!is.null(result)) {
        # log that run was sucessful
        readr::write_csv(
          x = data.frame(
            analysis_type = "dechallengeRechallenge",
            run_id = i,
            database_id = databaseId,
            date_time = as.character(Sys.time())
          ),
          file = file.path(saveDirectory, "tracker.csv"),
          append = file.exists(file.path(saveDirectory, "tracker.csv"))
        )

        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "dechallenge_rechallenge",
          andromedaObject = result$dechallengeRechallenge,
          tablePrefix = tablePrefix
        )
      }

      # run failed analysis
      message("Running rechallenge failed case analysis ", i)

      result <- tryCatch(
        {
          computeRechallengeFailCaseSeriesAnalyses(
            connectionDetails = connectionDetails,
            targetDatabaseSchema = targetDatabaseSchema,
            targetTable = targetTable,
            outcomeDatabaseSchema = outcomeDatabaseSchema,
            outcomeTable = outcomeTable,
            tempEmulationSchema = tempEmulationSchema,
            dechallengeRechallengeSettings = characterizationSettings$dechallengeRechallengeSettings[[i]],
            databaseId = databaseId,
            showSubjectId = showSubjectId
          )
        },
        error = function(e) {
          message(e);
          return(NULL)
        }
      )

      if (!is.null(result)) {
        # log that run was sucessful
        readr::write_csv(
          x = data.frame(
            analysis_type = "rechallengeFailCaseSeries",
            run_id = i,
            database_id = databaseId,
            date_time = as.character(Sys.time())
          ),
          file = file.path(saveDirectory, "tracker.csv"),
          append = file.exists(file.path(saveDirectory, "tracker.csv"))
        )

        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "rechallenge_fail_case_series",
          andromedaObject = result$rechallengeFailCaseSeries,
          tablePrefix = tablePrefix
        )
      }
    }
  }


  if (!is.null(characterizationSettings$aggregateCovariateSettings)) {
    ParallelLogger::logInfo("Running aggregate covariate analyses")

    for (i in 1:length(characterizationSettings$aggregateCovariateSettings)) {
      result <- tryCatch(
        {
          computeAggregateCovariateAnalyses(
            connectionDetails = connectionDetails,
            cdmDatabaseSchema = cdmDatabaseSchema,
            targetDatabaseSchema = targetDatabaseSchema,
            targetTable = targetTable,
            outcomeDatabaseSchema = outcomeDatabaseSchema,
            outcomeTable = outcomeTable,
            tempEmulationSchema = tempEmulationSchema,
            aggregateCovariateSettings = characterizationSettings$aggregateCovariateSettings[[i]],
            databaseId = databaseId,
            runId = i
          )
        },
        error = function(e) {
          message(e);
          return(NULL)
        }
      )

      if (!is.null(result)) {
        # log that run was successful
        readr::write_csv(
          x = data.frame(
            analysis_type = "aggregateCovariates",
            run_id = i,
            database_id = databaseId,
            date_time = as.character(Sys.time())
          ),
          file = file.path(saveDirectory, "tracker.csv"),
          append = file.exists(file.path(saveDirectory, "tracker.csv"))
        )

        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "settings",
          andromedaObject = result$settings,
          tablePrefix = tablePrefix
        )

        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "cohort_details",
          andromedaObject = result$cohortDetails,
          tablePrefix = tablePrefix
        )

        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "analysis_ref",
          andromedaObject = result$analysisRef,
          tablePrefix = tablePrefix
        )
        insertAndromedaToDatabase(
          connection = conn,
          databaseSchema = "main",
          tableName = "covariate_ref",
          andromedaObject = result$covariateRef,
          tablePrefix = tablePrefix
        )

        if (!is.null(result$covariates)) {
          insertAndromedaToDatabase(
            connection = conn,
            databaseSchema = "main",
            tableName = "covariates",
            andromedaObject = result$covariates,
            tablePrefix = tablePrefix
          )
        }

        if (!is.null(result$covariatesContinuous)) {
          insertAndromedaToDatabase(
            connection = conn,
            databaseSchema = "main",
            tableName = "covariates_continuous",
            andromedaObject = result$covariatesContinuous,
            tablePrefix = tablePrefix
          )
        }

        if (!is.null(result$timeRef)) {
          insertAndromedaToDatabase(
            connection = conn,
            databaseSchema = "main",
            tableName = "time_ref",
            andromedaObject = result$timeRef,
            tablePrefix = tablePrefix
          )
        }
      }
    }
  }


  invisible(saveDirectory)
}
