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

#' Create dechallenge rechallenge study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param outcomeIds   A list of cohortIds for the outcome cohorts
#' @param dechallengeStopInterval  An integer specifying the how much time to add to the cohort_end when determining whether the event starts during cohort and ends after
#' @param dechallengeEvaluationWindow An integer specifying the period of time after the cohort_end when you cannot see an outcome for a dechallenge success
#' @family {DechallengeRechallenge}
#' @return
#' A list with the settings
#'
#' @export
createDechallengeRechallengeSettings <- function(
    targetIds,
    outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30) {
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

  # check dechallengeStopInterval is numeric
  checkmate::assertNumeric(
    x = dechallengeStopInterval,
    lower = 0,
    finite = TRUE,
    any.missing = FALSE,
    len = 1,
    .var.name = "dechallengeStopInterval",
    add = errorMessages
  )

  # check dechallengeEvaluationWindowl is numeric
  checkmate::assertNumeric(
    x = dechallengeEvaluationWindow,
    lower = 0,
    finite = TRUE,
    any.missing = FALSE,
    len = 1,
    .var.name = "dechallengeEvaluationWindow",
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

  class(result) <- "dechallengeRechallengeSettings"
  return(result)
}

#' Compute dechallenge rechallenge study
#'
#' @template ConnectionDetails
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param settings   The settings for the timeToEvent study
#' @param databaseId An identifier for the database (string)
#' @param outputFolder A directory to save the results as csv files
#' @param minCellCount The minimum cell value to display, values less than this will be replaced by -1
#' @param ... extra inputs
#' @family {DechallengeRechallenge}
#' @return
#' An \code{Andromeda::andromeda()} object containing the dechallenge rechallenge results
#'
#' @export
computeDechallengeRechallengeAnalyses <- function(
    connectionDetails = NULL,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema,
    outcomeTable = targetTable,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder = file.path(getwd(), "results"),
    minCellCount = 0,
    ...) {
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
  .checkDechallengeRechallengeSettings(
    settings = settings,
    errorMessages = errorMessages
  )

  valid <- checkmate::reportAssertions(
    collection = errorMessages
  )

  if (valid) {
    # inputs all pass if getting here
    message("Inputs checked")

    start <- Sys.time()

    connection <- DatabaseConnector::connect(
      connectionDetails = connectionDetails
    )
    on.exit(
      DatabaseConnector::disconnect(connection)
    )

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
      target_ids = paste(settings$targetCohortDefinitionIds, sep = "", collapse = ","),
      outcome_ids = paste(settings$outcomeCohortDefinitionIds, sep = "", collapse = ","),
      dechallenge_stop_interval = settings$dechallengeStopInterval,
      dechallenge_evaluation_window = settings$dechallengeEvaluationWindow
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )

    sql <- "select * from #challenge;"
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    result <- DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      andromeda = Andromeda::andromeda(),
      andromedaTableName = "dechallengeRechallenge",
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
        length(settings$targetCohortDefinitionIds), " target ids and ",
        length(settings$outcomeCohortDefinitionIds), " outcome ids took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    # export results to csv
    message("exporting to csv file")
    exportDechallengeRechallengeToCsv(
      result = result,
      saveDirectory = outputFolder,
      minCellCount = minCellCount
    )

    return(invisible(TRUE))
  }
}


#' Compute fine the subjects that fail the dechallenge rechallenge study
#'
#' @template ConnectionDetails
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param settings   The settings for the timeToEvent study
#' @param databaseId An identifier for the database (string)
#' @param showSubjectId if F then subject_ids are hidden (recommended if sharing results)
#' @param outputFolder A directory to save the results as csv files
#' @param minCellCount The minimum cell value to display, values less than this will be replaced by -1
#' @param ... extra inputs
#' @family {DechallengeRechallenge}
#' @return
#' An \code{Andromeda::andromeda()} object with the case series details of the failed rechallenge
#'
#' @export
computeRechallengeFailCaseSeriesAnalyses <- function(
    connectionDetails = NULL,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema,
    outcomeTable = targetTable,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    showSubjectId = F,
    outputFolder = file.path(getwd(), "results"),
    minCellCount = 0,
    ...) {
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
  .checkDechallengeRechallengeSettings(
    settings = settings,
    errorMessages = errorMessages
  )

  valid <- checkmate::reportAssertions(errorMessages)

  if (valid) {
    # inputs all pass if getting here
    message("Inputs checked")

    start <- Sys.time()

    connection <- DatabaseConnector::connect(
      connectionDetails = connectionDetails
    )
    on.exit(
      DatabaseConnector::disconnect(connection)
    )

    message("Computing dechallenge rechallenge fails results")
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
      target_ids = paste(settings$targetCohortDefinitionIds, sep = "", collapse = ","),
      outcome_ids = paste(settings$outcomeCohortDefinitionIds, sep = "", collapse = ","),
      dechallenge_stop_interval = settings$dechallengeStopInterval,
      dechallenge_evaluation_window = settings$dechallengeEvaluationWindow,
      show_subject_id = showSubjectId
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )

    sql <- "select * from #fail_case_series;"
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = connection@dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    result <- DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      andromeda = Andromeda::andromeda(),
      andromedaTableName = "rechallengeFailCaseSeries",
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
        length(settings$targetCohortDefinitionIds), " target IDs and ",
        length(settings$outcomeCohortDefinitionIds), " outcome IDs took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    # add the csv export here
    message("exporting to csv file")
    exportRechallengeFailCaseSeriesToCsv(
      result = result,
      saveDirectory = outputFolder
    )

    return(invisible(TRUE))
  }
}

getDechallengeRechallengeJobs <- function(
    characterizationSettings,
    threads) {
  characterizationSettings <- characterizationSettings$dechallengeRechallengeSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }
  ind <- 1:length(characterizationSettings)
  targetIds <- lapply(ind, function(i) {
    characterizationSettings[[i]]$targetCohortDefinitionIds
  })
  outcomeIds <- lapply(ind, function(i) {
    characterizationSettings[[i]]$outcomeCohortDefinitionIds
  })
  dechallengeStopIntervals <- lapply(ind, function(i) {
    characterizationSettings[[i]]$dechallengeStopInterval
  })
  dechallengeEvaluationWindows <- lapply(ind, function(i) {
    characterizationSettings[[i]]$dechallengeEvaluationWindow
  })

  # get all combinations of TnOs, then split by treads

  combinations <- do.call(
    what = "rbind",
    args =
      lapply(
        1:length(targetIds),
        function(i) {
          result <- expand.grid(
            targetId = targetIds[[i]],
            outcomeId = outcomeIds[[i]]
          )
          result$dechallengeStopInterval <- dechallengeStopIntervals[[i]]
          result$dechallengeEvaluationWindow <- dechallengeEvaluationWindows[[i]]
          return(result)
        }
      )
  )
  # find out whether more Ts or more Os
  tcount <- nrow(
    combinations %>%
      dplyr::count(
        .data$targetId,
        .data$dechallengeStopInterval,
        .data$dechallengeEvaluationWindow
      )
  )

  ocount <- nrow(
    combinations %>%
      dplyr::count(
        .data$outcomeId,
        .data$dechallengeStopInterval,
        .data$dechallengeEvaluationWindow
      )
  )

  if (threads > max(tcount, ocount)) {
    message("Tnput parameter threads greater than number of targets and outcomes")
    message(paste0("Only using ", max(tcount, ocount), " threads for TimeToEvent"))
  }

  if (tcount >= ocount) {
    threadDf <- combinations %>%
      dplyr::count(
        .data$targetId,
        .data$dechallengeStopInterval,
        .data$dechallengeEvaluationWindow
      )
    threadDf$thread <- rep(1:threads, ceiling(tcount / threads))[1:tcount]
    mergeColumn <- c("targetId", "dechallengeStopInterval", "dechallengeEvaluationWindow")
  } else {
    threadDf <- combinations %>%
      dplyr::count(
        .data$outcomeId,
        .data$dechallengeStopInterval,
        .data$dechallengeEvaluationWindow
      )
    threadDf$thread <- rep(1:threads, ceiling(ocount / threads))[1:ocount]
    mergeColumn <- c("outcomeId", "dechallengeStopInterval", "dechallengeEvaluationWindow")
  }

  combinations <- merge(combinations, threadDf, by = mergeColumn)
  sets <- lapply(
    X = 1:max(threadDf$thread),
    FUN = function(i) {
      createDechallengeRechallengeSettings(
        targetIds = unique(combinations$targetId[combinations$thread == i]),
        outcomeIds = unique(combinations$outcomeId[combinations$thread == i]),
        dechallengeStopInterval = unique(combinations$dechallengeStopInterval[combinations$thread == i]),
        dechallengeEvaluationWindow = unique(combinations$dechallengeEvaluationWindow[combinations$thread == i])
      )
    }
  )

  # recreate settings
  settings <- c()
  for (i in 1:length(sets)) {
    settings <- rbind(
      settings,
      data.frame(
        functionName = "computeDechallengeRechallengeAnalyses",
        settings = as.character(ParallelLogger::convertSettingsToJson(
          sets[[i]]
        )),
        executionFolder = paste0("dr_", i),
        jobId = paste0("dr_", i)
      )
    )
    settings <- rbind(
      settings,
      data.frame(
        functionName = "computeRechallengeFailCaseSeriesAnalyses",
        settings = as.character(ParallelLogger::convertSettingsToJson(
          sets[[i]]
        )),
        executionFolder = paste0("rfcs_", i),
        jobId = paste0("rfcs_", i)
      )
    )
  }

  return(settings)
}
