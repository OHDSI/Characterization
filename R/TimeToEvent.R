# Copyright 2022 Observational Health Data Sciences and Informatics
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
#'
#' @return
#' An list with the time to event settings
#'
#' @export
createTimeToEventSettings <- function(
  targetIds,
  outcomeIds
){

  # check indicationIds
  errorMessages <- checkmate::makeAssertCollection()
  # check targetIds is a vector of int/double
  .checkCohortIds(
    cohortIds = targetIds,
    type = 'target',
    errorMessages = errorMessages
  )
  # check outcomeIds is a vector of int/double
  .checkCohortIds(
    cohortIds = outcomeIds,
    type = 'outcome',
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(errorMessages)


  # create data.frame with all combinations
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds
  )

  class(result) <- 'timeToEventSettings'
  return(result)
}

#' Compute time to event study
#'
#' @template ConnectionDetails
#' @template Connection
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param cdmDatabaseSchema The database schema containing the OMOP CDM data
#' @param timeToEventSettings   The settings for the timeToEvent study
#' @param databaseId An identifier for the database (string)
#'
#' @return
#' An \code{Andromeda::andromeda()} object containing the time to event results.
#'
#' @export
computeTimeToEventAnalyses <- function(
  connectionDetails = NULL,
  connection = NULL,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema = targetDatabaseSchema,
  outcomeTable = targetTable,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cdmDatabaseSchema,
  timeToEventSettings,
  databaseId = 'database 1'
) {

  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      .checkConnectionDetails(connectionDetails)
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  # check inputs
  errorMessages <- checkmate::makeAssertCollection()
  .checkCohortDetails(
    cohortDatabaseSchema = targetDatabaseSchema,
    cohortTable = targetTable,
    type = 'target',
    errorMessages =  errorMessages
  )
  .checkCohortDetails(
    cohortDatabaseSchema = outcomeDatabaseSchema,
    cohortTable = outcomeTable,
    type = 'outcome',
    errorMessages =  errorMessages
  )
  .checkTempEmulationSchema(
    tempEmulationSchema = tempEmulationSchema,
    errorMessages =  errorMessages
  )
  .checkTimeToEventSettings(
    settings = timeToEventSettings,
    errorMessages =  errorMessages
  )

  valid <- checkmate::reportAssertions(errorMessages)

  if(valid){
    start <- Sys.time()

    # upload table to #cohort_settings
    message("Uploading #cohort_settings")

    pairs <- expand.grid(
      targetCohortDefinitionId = timeToEventSettings$targetIds,
      outcomeCohortDefinitionId = timeToEventSettings$outcomeIds
    )

    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#cohort_settings",
      data = pairs,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
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
      sql = sql
    )

    sql <- 'select * from #two_tte_summary;'
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect =  connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    result <- DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = Andromeda::andromeda(),
      andromedaTableName = 'timeToEvent',
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
      sql = sql, progressBar = FALSE,
      reportOverallTime = FALSE
    )

    delta <- Sys.time() - start

    message(
      paste0(
        "Computing time-to-event for ",
        nrow(timeToEventSettings),
        "T-O pairs took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    return(result)
  }
}
