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

#' Create dechallenge rechallenge study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param outcomeIds   A list of cohortIds for the outcome cohorts
#' @param dechallengeStopInterval  An integer specifying the how much time to add to the cohort_end when determining whether the event starts during cohort and ends after
#' @param dechallengeEvaluationWindow An integer specifying the period of time after the cohort_end when you cannot see an outcome for a dechallenge success
#'
#' @return
#' A list with the settings
#'
#' @export
createDechallengeRechallengeSettings <- function(
  targetIds,
  outcomeIds,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 30
){

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

  # check dechallengeStopInterval is numeric
  checkmate::assertNumeric(
    x = dechallengeStopInterval,
    lower = 0,
    finite = TRUE,
    any.missing = FALSE,
    len = 1,
    .var.name = 'dechallengeStopInterval',
    add = errorMessages
  )

  # check dechallengeEvaluationWindowl is numeric
  checkmate::assertNumeric(
    x = dechallengeEvaluationWindow,
    lower = 0,
    finite = TRUE,
    any.missing = FALSE,
    len = 1,
    .var.name = 'dechallengeEvaluationWindow',
    add = errorMessages
  )

  checkmate::reportAssertions(errorMessages)

  # create data.frame with all combinations
  result <- list(
    targetCohortDefinitionIds = targetIds,
    outcomeCohortDefinitionIds = outcomeIds,
    dechallengeStopInterval = dechallengeStopInterval,
    dechallengeEvaluationWindow = dechallengeEvaluationWindow
  )

  class(result) <- 'dechallengeRechallengeSettings'
  return(result)
}

#' Compute dechallenge rechallenge study
#'
#' @template ConnectionDetails
#' @template Connection
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param dechallengeRechallengeSettings   The settings for the timeToEvent study
#' @param databaseId An identifier for the database (string)
#'
#' @return
#' An \code{Andromeda::andromeda()} object containing the dechallenge rechallenge results
#'
#' @export
computeDechallengeRechallengeAnalyses <- function(
  connectionDetails = NULL,
  connection = NULL,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema = targetDatabaseSchema,
  outcomeTable =  targetTable,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  dechallengeRechallengeSettings,
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
  .checkDechallengeRechallengeSettings(
    settings = dechallengeRechallengeSettings,
    errorMessages =  errorMessages
    )

  valid <- checkmate::reportAssertions(
    collection = errorMessages
    )

  if(valid){
    # inputs all pass if getting here
    message("Inputs checked")

    start <- Sys.time()

    message("Computing dechallenge rechallenge results")
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "DechallengeRechallenge.sql",
      packageName = "Characterization",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      database_id = databaseId,
      target_database_schema = targetDatabaseSchema,
      target_table = targetTable,
      outcome_database_schema = outcomeDatabaseSchema,
      outcome_table = outcomeTable,
      target_ids = paste(dechallengeRechallengeSettings$targetCohortDefinitionIds, sep='', collapse = ','),
      outcome_ids = paste(dechallengeRechallengeSettings$outcomeCohortDefinitionIds, sep='', collapse = ','),
      dechallenge_stop_interval = dechallengeRechallengeSettings$dechallengeStopInterval,
      dechallenge_evaluation_window = dechallengeRechallengeSettings$dechallengeEvaluationWindow
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )

    sql <- 'select * from #challenge;'
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect =  connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    result <- DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      andromeda = Andromeda::andromeda(),
      andromedaTableName = 'dechallengeRechallenge',
      sql = sql,
      snakeCaseToCamelCase = TRUE
    )

    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "DropDechallengeRechallenge.sql",
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
        "Computing dechallenge rechallenge for ",
        length(dechallengeRechallengeSettings$targetCohortDefinitionIds), " target ids and ",
        length(dechallengeRechallengeSettings$outcomeCohortDefinitionIds),"outcome ids took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    return(result)
  }
}


#' Compute fine the subjects that fail the dechallenge rechallenge study
#'
#' @template ConnectionDetails
#' @template Connection
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param dechallengeRechallengeSettings   The settings for the timeToEvent study
#' @param databaseId An identifier for the database (string)
#' @param showSubjectId if F then subject_ids are hidden (recommended if sharing results)
#'
#' @return
#' An \code{Andromeda::andromeda()} object with the case series details of the failed rechallenge
#'
#' @export
computeRechallengeFailCaseSeriesAnalyses <- function(
  connectionDetails = NULL,
  connection = NULL,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema = targetDatabaseSchema,
  outcomeTable =  targetTable,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  dechallengeRechallengeSettings,
  databaseId = 'database 1',
  showSubjectId = F
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
  .checkDechallengeRechallengeSettings(
    settings = dechallengeRechallengeSettings,
    errorMessages =  errorMessages
  )

  valid <- checkmate::reportAssertions(errorMessages)

  if(valid){
    # inputs all pass if getting here
    message("Inputs checked")

    start <- Sys.time()

    message("Computing dechallenge rechallenge results")
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "RechallengeFailCaseSeries.sql",
      packageName = "Characterization",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      database_id = databaseId,
      target_database_schema = targetDatabaseSchema,
      target_table = targetTable,
      outcome_database_schema = outcomeDatabaseSchema,
      outcome_table = outcomeTable,
      target_ids = paste(dechallengeRechallengeSettings$targetCohortDefinitionIds, sep='', collapse = ','),
      outcome_ids = paste(dechallengeRechallengeSettings$outcomeCohortDefinitionIds, sep='', collapse = ','),
      dechallenge_stop_interval = dechallengeRechallengeSettings$dechallengeStopInterval,
      dechallenge_evaluation_window = dechallengeRechallengeSettings$dechallengeEvaluationWindow,
      show_subject_id = showSubjectId
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )

    sql <- 'select * from #fail_case_series;'
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect =  connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    result <- DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      andromeda = Andromeda::andromeda(),
      andromedaTableName = 'rechallengeFailCaseSeries',
      sql = sql,
      snakeCaseToCamelCase = TRUE
    )

    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "DropRechallengeFailCaseSeries.sql",
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
        "Computing dechallenge failed case series for ",
        length(dechallengeRechallengeSettings$targetCohortDefinitionIds), " target IDs and ",
        length(dechallengeRechallengeSettings$outcomeCohortDefinitionIds)," outcome IDs took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    return(result)
  }
}
