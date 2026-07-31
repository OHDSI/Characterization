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
#' @param studyPopulationSettings An object created using \code{createStudyPopulationSettings} of a list of \code{createStudyPopulationSettings} that specifies cohort inclusion criteria
#' @param outcomeIds   A list of cohortIds for the outcome cohorts
#' @param dechallengeStopInterval  An integer specifying the how much time to add to the cohort_end when determining whether the event starts during cohort and ends after
#' @param dechallengeEvaluationWindow An integer specifying the period of time after the cohort_end when you cannot see an outcome for a dechallenge success
#' @family DechallengeRechallenge
#'
#' @return
#' A list with the settings
#'
#' @examples
#' drSet <- createDechallengeRechallengeSettings(
#'   studyPopulationSettings = createStudyPopulationSettings(
#'     targetIds = c(1,2),
#'     limitToFirstInNDays = 0,
#'     minPriorObservation = 0
#'     ),
#'   outcomeIds = 3
#' )
#'
#'
#' @export
createDechallengeRechallengeSettings <- function(
    studyPopulationSettings,
    outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
    ) {
  errorMessages <- checkmate::makeAssertCollection()
  # check targetIds is a vector of int/double
  #.checkCohortIds(
  #  cohortIds = targetIds,
  #  type = "target",
  #  errorMessages = errorMessages
  #)
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
    studyPopulationSettings = combineStudyPopulationSettings(studyPopulationSettings),
    outcomeIds = outcomeIds,
    dechallengeStopInterval = dechallengeStopInterval,
    dechallengeEvaluationWindow = dechallengeEvaluationWindow
  )

  class(result) <- "dechallengeRechallengeSettings"
  return(result)
}


computeDechallengeRechallengeAnalyses <- function(
    connectionDetails = NULL,
    targetDatabaseSchema, # not needed
    targetTable,    # not needed
    outcomeDatabaseSchema = targetDatabaseSchema,
    outcomeTable = targetTable,
    characterizationDatabaseSchema, # updated
    characterizationTable, # updated
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
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
    cohortDatabaseSchema = characterizationDatabaseSchema,
    cohortTable = characterizationTable,
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
  #.checkDechallengeRechallengeSettings(
  #  settings = settings,
  #  errorMessages = errorMessages
  #)

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
      characterization_database_schema = characterizationDatabaseSchema, # updated
      characterization_table = characterizationTable, # updated,
      outcome_database_schema = outcomeDatabaseSchema,
      outcome_table = outcomeTable,
      characterization_target_ids = paste(settings$characterizationTargetIds, sep = "", collapse = ","),
      outcome_ids = paste(settings$outcomeIds, sep = "", collapse = ","),
      dechallenge_stop_interval = settings$dechallengeStopInterval,
      dechallenge_evaluation_window = settings$dechallengeEvaluationWindow
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = progressBar
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
      sql = sql,
      progressBar = progressBar,
      reportOverallTime = FALSE
    )

    delta <- Sys.time() - start

    message(
      paste0(
        "Computing dechallenge rechallenge for ",
        length(settings$characterizationTargetIds), " target ids and ",
        length(settings$outcomeIds), " outcome ids took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    # export results to csv
    message("exporting to andomeda")
    saveCharacterizationAndromeda(
      andromeda = result,
      outputFolder = outputFolder
    )

    return(invisible(TRUE))
  }
}


computeRechallengeFailCaseSeriesAnalyses <- function(
    connectionDetails = NULL,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema,
    outcomeTable = targetTable,
    characterizationDatabaseSchema, # updated
    characterizationTable, # updated
    targetSettingsTable, # added
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    showSubjectId = FALSE,
    outputFolder,
    minCellCount = 0,
    progressBar = interactive(),
    executionId,
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
  #.checkDechallengeRechallengeSettings(
  #  settings = settings,
  #  errorMessages = errorMessages
  #)

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

    # TODO: lookup targetIds based on settings$studyPopulationSettings


    message("Computing dechallenge rechallenge fails results")
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "RechallengeFailCaseSeries.sql",
      packageName = "Characterization",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      database_id = databaseId,
      characterization_database_schema = characterizationDatabaseSchema, # updated
      characterization_table = characterizationTable, # updated
      target_settings = targetSettingsTable, #updated
      target_database_schema = targetDatabaseSchema,
      target_table = targetTable,
      outcome_database_schema = outcomeDatabaseSchema,
      outcome_table = outcomeTable,
      characterization_target_ids = paste(settings$characterizationTargetIds, sep = "", collapse = ","),
      outcome_ids = paste(settings$outcomeIds, sep = "", collapse = ","),
      dechallenge_stop_interval = settings$dechallengeStopInterval,
      dechallenge_evaluation_window = settings$dechallengeEvaluationWindow,
      show_subject_id = showSubjectId
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = progressBar
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
      sql = sql,
      progressBar = progressBar,
      reportOverallTime = FALSE
    )

    delta <- Sys.time() - start

    message(
      paste0(
        "Computing dechallenge failed case series for ",
        length(settings$characterizationTargetIds), " target IDs and ",
        length(settings$outcomeIds), " outcome IDs took ",
        signif(delta, 3), " ",
        attr(delta, "units")
      )
    )

    # add the csv export here
    message("exporting to andromeda")
    saveCharacterizationAndromeda(
      andromeda = result,
      outputFolder = outputFolder
    )

    return(invisible(TRUE))
  }
}

getDechallengeRechallengeJobs <- function(
    characterizationSettings,
    nTargetJobs) {
  characterizationSettings <- characterizationSettings$dechallengeRechallengeSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }
  ind <- 1:length(characterizationSettings)
  characterizationTargetIds <- lapply(ind, function(i) {
    characterizationSettings[[i]]$characterizationTargetIds
  })
  outcomeIds <- lapply(ind, function(i) {
    characterizationSettings[[i]]$outcomeIds
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
        1:length(characterizationTargetIds),
        function(i) {
          result <- expand.grid(
            characterizationTargetId = characterizationTargetIds[[i]],
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
        .data$characterizationTargetId,
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

  if (nTargetJobs > max(tcount, ocount)) {
    message("Input parameter nTargetJobs greater than number of targets and outcomes")
    message(paste0("Only using ", max(tcount, ocount), " nTargetJobs for DechallengeRechallenge"))
  }

  if (tcount >= ocount) {
    threadDf <- combinations %>%
      dplyr::count(
        .data$characterizationTargetId,
        .data$dechallengeStopInterval,
        .data$dechallengeEvaluationWindow
      )
    threadDf$nTargetJobs <- rep(1:nTargetJobs, ceiling(tcount / nTargetJobs))[1:tcount]
    mergeColumn <- c("characterizationTargetId", "dechallengeStopInterval", "dechallengeEvaluationWindow")
  } else {
    threadDf <- combinations %>%
      dplyr::count(
        .data$outcomeId,
        .data$dechallengeStopInterval,
        .data$dechallengeEvaluationWindow
      )
    threadDf$nTargetJobs <- rep(1:nTargetJobs, ceiling(ocount / nTargetJobs))[1:ocount]
    mergeColumn <- c("outcomeId", "dechallengeStopInterval", "dechallengeEvaluationWindow")
  }

  combinations <- merge(combinations, threadDf, by = mergeColumn)


  # create settings based on dechallengeStopInterval/dechallengeEvaluationWindow

  settingCols <- c("dechallengeStopInterval", "dechallengeEvaluationWindow")
  executionSettings <- combinations %>%
    dplyr::select(dplyr::all_of(settingCols)) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      settingId = dplyr::row_number()
    )
  combinations <- merge(combinations, executionSettings, by = settingCols)


  # recreate settings
  settings <- c()
  for (settingId in unique(combinations$settingId)) {
    for (targetJobId in unique(combinations$nTargetJobs)){

      restrictedCombo <- combinations %>%
        dplyr::filter(.data$settingId == !!settingId) %>%
        dplyr::filter(.data$nTargetJobs == !!targetJobId)


      settings <- rbind(
        settings,
        data.frame(
          functionName = "computeDechallengeRechallengeAnalyses",
          settings = as.character(ParallelLogger::convertSettingsToJson(
            list(
              characterizationTargetIds = unique(restrictedCombo$characterizationTargetId),
              outcomeIds = unique(restrictedCombo$outcomeId),
              dechallengeStopInterval = unique(restrictedCombo$dechallengeStopInterval),
              dechallengeEvaluationWindow = unique(restrictedCombo$dechallengeEvaluationWindow)
            )
          )),
          executionFolder = paste("dr", targetJobId,
                                   unique(restrictedCombo$dechallengeStopInterval),
                                   unique(restrictedCombo$dechallengeEvaluationWindow),
                                  sep = '_'),
          jobId = paste("dr", targetJobId,
                        unique(restrictedCombo$dechallengeStopInterval),
                        unique(restrictedCombo$dechallengeEvaluationWindow),
                        sep = '_')
        )
      )
      settings <- rbind(
        settings,
        data.frame(
          functionName = "computeRechallengeFailCaseSeriesAnalyses",
          settings = as.character(ParallelLogger::convertSettingsToJson(
            list(
              characterizationTargetIds = unique(restrictedCombo$characterizationTargetId),
              outcomeIds = unique(restrictedCombo$outcomeId),
              dechallengeStopInterval = unique(restrictedCombo$dechallengeStopInterval),
              dechallengeEvaluationWindow = unique(restrictedCombo$dechallengeEvaluationWindow)
            )
          )),
          executionFolder = paste("rfcs", targetJobId,
                                  unique(restrictedCombo$dechallengeStopInterval),
                                  unique(restrictedCombo$dechallengeEvaluationWindow),
                                  sep = '_'),
          jobId = paste("rfcs", targetJobId,
                        unique(restrictedCombo$dechallengeStopInterval),
                        unique(restrictedCombo$dechallengeEvaluationWindow),
                        sep = '_')
        )
      )
    }
  }

  return(settings)
}
