# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of Characterization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Create time to event study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param outcomeIds   A list of cohortIds for the outcome cohorts
#' @family TimeToEvent
#'
#' @return
#' An list with the time to event settings
#'
#' @examples
#' # example code
#'
#' tteSet <- createTimeToEventSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = 3
#' )
#'
#'
#' @export
createTimeToEventSettings <- function(
    targetIds,
    outcomeIds) {
  # check indicationIds
  errorMessages <- checkmate::makeAssertCollection()
  # check targetIds is a vector of int/double
  .checkCohortIds(
    cohortIds = targetIds,
    type = "target",
    errorMessages = errorMessages
  )
  # check outcomeIds is a vector of int/double
  .checkCohortIds(
    cohortIds = outcomeIds,
    type = "outcome",
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(errorMessages)


  # create data.frame with all combinations
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds
  )

  class(result) <- "timeToEventSettings"
  return(result)
}

#' Compute time to event study
#'
#' @template ConnectionDetails
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param cdmDatabaseSchema The database schema containing the OMOP CDM data
#' @param settings   The settings for the timeToEvent study
#' @param databaseId An identifier for the database (string)
#' @param outputFolder A directory to save the results as csv files
#' @param minCellCount The minimum cell value to display, values less than this will be replaced by -1
#' @param progressBar Whether to display a progress bar while the analysis is running
#' @param ... extra inputs
#' @family TimeToEvent
#'
#' @return
#' An \code{Andromeda::andromeda()} object containing the time to event results.
#'
#' @examples
#' # example code
#'
#' conDet <- exampleOmopConnectionDetails()
#'
#' tteSet <- createTimeToEventSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = 3
#' )
#'
#' result <- computeTimeToEventAnalyses(
#'   connectionDetails = conDet,
#'   targetDatabaseSchema = 'main',
#'   targetTable = 'cohort',
#'   cdmDatabaseSchema = 'main',
#'   settings = tteSet,
#'   outputFolder = file.path(tempdir(), 'tte')
#' )
#'
#'
#'
#' @export
computeTimeToEventAnalyses <- function(
    connectionDetails = NULL,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema,
    outcomeTable = targetTable,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cdmDatabaseSchema,
    settings,
    databaseId = "database 1",
    outputFolder,
    minCellCount = 0,
    progressBar = interactive(),
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  # check inputs
  errorMessages <- checkmate::makeAssertCollection()
  .checkConnectionDetails(connectionDetails, errorMessages)
  .checkCohortDetails(
    cohortDatabaseSchema = targetDatabaseSchema,
    cohortTable = targetTable,
    type = "target",
    errorMessages = errorMessages
  )
  .checkCohortDetails(
    cohortDatabaseSchema = outcomeDatabaseSchema,
    cohortTable = outcomeTable,
    type = "outcome",
    errorMessages = errorMessages
  )
  .checkTempEmulationSchema(
    tempEmulationSchema = tempEmulationSchema,
    errorMessages = errorMessages
  )
  .checkTimeToEventSettings(
    settings = settings,
    errorMessages = errorMessages
  )

  valid <- checkmate::reportAssertions(errorMessages)

  if (valid) {
    start <- Sys.time()

    connection <- DatabaseConnector::connect(
      connectionDetails = connectionDetails
    )
    on.exit(
      DatabaseConnector::disconnect(connection)
    )

    # upload table to #cohort_settings
    message("Uploading #cohort_settings")

    pairs <- expand.grid(
      targetCohortDefinitionId = settings$targetIds,
      outcomeCohortDefinitionId = settings$outcomeIds
    )

    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#cohort_settings",
      data = pairs,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = progressBar,
      camelCaseToSnakeCase = TRUE
    )

    message("Computing time to event results")
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "TimeToEvent.sql",
      packageName = "Characterization",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      database_id = databaseId,
      cdm_database_schema = cdmDatabaseSchema,
      target_database_schema = targetDatabaseSchema,
      target_table = targetTable,
      outcome_database_schema = outcomeDatabaseSchema,
      outcome_table = outcomeTable
    )

    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = progressBar
    )

    sql <- "select * from #two_tte_summary;"
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    result <- DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = Andromeda::andromeda(),
      andromedaTableName = "timeToEvent",
      snakeCaseToCamelCase = TRUE
    )

    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "DropTimeToEvent.sql",
      packageName = "Characterization",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = progressBar,
      reportOverallTime = FALSE
    )

    delta <- Sys.time() - start

    message(
      paste0(
        "Computing time-to-event for ",
        nrow(pairs),
        " T-O pairs took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    # add the csv export here
    message("exporting to csv file")
    exportTimeToEventToCsv(
      result = result,
      saveDirectory = outputFolder,
      minCellCount = minCellCount
    )

    return(invisible(TRUE))
  }
}

# code that takes a characterizationSettings list, extracts
# timeToEvent settings and then converts into distinct jobs
# based on the number of threads
getTimeToEventJobs <- function(
    characterizationSettings,
    threads) {
  characterizationSettings <- characterizationSettings$timeToEventSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }
  ind <- 1:length(characterizationSettings)
  targetIds <- lapply(ind, function(i) {
    characterizationSettings[[i]]$targetIds
  })
  outcomeIds <- lapply(ind, function(i) {
    characterizationSettings[[i]]$outcomeIds
  })

  # get all combinations of TnOs, then split by treads

  tnos <- do.call(
    what = "rbind",
    args =
      lapply(
        1:length(targetIds),
        function(i) {
          expand.grid(
            targetId = targetIds[[i]],
            outcomeId = outcomeIds[[i]]
          )
        }
      )
  )
  # find out whether more Ts or more Os
  tcount <- length(unique(tnos$targetId))
  ocount <- length(unique(tnos$outcomeId))

  if (threads > max(tcount, ocount)) {
    message("Tnput parameter threads greater than number of targets and outcomes")
    message(paste0("Only using ", max(tcount, ocount), " threads for TimeToEvent"))
  }

  if (tcount >= ocount) {
    threadDf <- data.frame(
      targetId = unique(tnos$targetId),
      thread = rep(1:threads, ceiling(tcount / threads))[1:tcount]
    )
    mergeColumn <- "targetId"
  } else {
    threadDf <- data.frame(
      outcomeId = unique(tnos$outcomeId),
      thread = rep(1:threads, ceiling(ocount / threads))[1:ocount]
    )
    mergeColumn <- "outcomeId"
  }

  tnos <- merge(tnos, threadDf, by = mergeColumn)
  sets <- lapply(
    X = 1:max(threadDf$thread),
    FUN = function(i) {
      createTimeToEventSettings(
        targetIds = unique(tnos$targetId[tnos$thread == i]),
        outcomeIds = unique(tnos$outcomeId[tnos$thread == i])
      )
    }
  )

  # recreate settings
  settings <- c()
  for (i in 1:length(sets)) {
    settings <- rbind(
      settings,
      data.frame(
        functionName = "computeTimeToEventAnalyses",
        settings = as.character(ParallelLogger::convertSettingsToJson(
          sets[[i]]
        )),
        executionFolder = paste0("tte_", i),
        jobId = paste0("tte_", i)
      )
    )
  }

  return(settings)
}
