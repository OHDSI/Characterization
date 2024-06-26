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

#' Create aggregate covariate study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param outcomeIds  A list of cohortIds for the outcome cohorts
#' @param minPriorObservation The minimum time in the database a patient in the target cohorts must be observed prior to index
#' @template timeAtRisk
#' @param covariateSettings   An object created using \code{FeatureExtraction::createCovariateSettings}
#' @param minCharacterizationMean The minimum mean value for characterization output. Values below this will be cut off from output. This
#'                                will help reduce the file size of the characterization output, but will remove information
#'                                on covariates that have very low values. The default is 0.
#'
#' @return
#' A list with the settings
#'
#' @export
createAggregateCovariateSettings <- function(
    targetIds,
    outcomeIds,
    minPriorObservation = 0,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings,
    minCharacterizationMean = 0) {
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

  # check TAR
  .checkTimeAtRisk(
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    errorMessages = errorMessages
  )

  # check covariateSettings
  .checkCovariateSettings(
    covariateSettings = covariateSettings,
    errorMessages = errorMessages
  )

  # check minPriorObservation
  .checkMinPriorObservation(
    minPriorObservation = minPriorObservation,
    errorMessages = errorMessages
  )

  checkmate::reportAssertions(errorMessages)

  # create list
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = minPriorObservation,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    covariateSettings = covariateSettings,
    minCharacterizationMean = minCharacterizationMean
  )

  class(result) <- "aggregateCovariateSettings"
  return(result)
}

#' Compute aggregate covariate study
#'
#' @template ConnectionDetails
#' @param cdmDatabaseSchema The schema with the OMOP CDM data
#' @param cdmVersion  The version of the OMOP CDM
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param aggregateCovariateSettings   The settings for the AggregateCovariate study
#' @param databaseId Unique identifier for the database (string)
#' @param runId  Unique identifier for the tar and covariate setting
#'
#' @return
#' The descriptive results for each target cohort in the settings.
#'
#' @export
computeAggregateCovariateAnalyses <- function(
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cdmVersion = 5,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema, # remove
    outcomeTable = targetTable, # remove
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    aggregateCovariateSettings,
    databaseId = "database 1",
    runId = 1) {
  # check inputs

  start <- Sys.time()

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # select T, O, create TnO, TnOc, Onprior T
  # into temp table #agg_cohorts
  createCohortsOfInterest(
    connection = connection,
    dbms = connectionDetails$dbms,
    cdmDatabaseSchema = cdmDatabaseSchema,
    aggregateCovariateSettings,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    tempEmulationSchema
  )

  ## get counts
  sql <- "select cohort_definition_id, count(*) row_count, count(distinct subject_id) person_count from #agg_cohorts group by cohort_definition_id;"
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connectionDetails$dbms
  )
  counts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = T,
  )

  message("Computing aggregate covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts",
    cohortTableIsTemp = T,
    cohortId = -1,
    covariateSettings = aggregateCovariateSettings$covariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
  )
  # adding counts as a new table
  result$cohortCounts <- counts

  # add databaseId and runId to each table in results
  # could add settings table with this and just have setting id
  # as single extra column?

  for (tableName in names(result)) {
    result[[tableName]] <- result[[tableName]] %>%
      dplyr::mutate(
        runId = !!runId,
        databaseId = !!databaseId
      ) %>%
      dplyr::relocate(
        "databaseId",
        "runId"
      )
  }

  # cohort details:

  result$cohortDetails <- DatabaseConnector::querySql(
    connection = connection,
    sql = SqlRender::translate(
      sql = " select * from #cohort_details;",
      targetDialect = connectionDetails$dbms
    ),
    snakeCaseToCamelCase = T
  ) %>%
    dplyr::mutate(
      runId = !!runId,
      databaseId = !!databaseId
    ) %>%
    dplyr::relocate(
      "databaseId",
      "runId"
    )

  # settings:
  # run_id, database_id, covariate_setting_json,
  # riskWindowStart, startAnchor, riskWindowEnd, endAnchor

  covariateSettingsJson <- as.character(
    ParallelLogger::convertSettingsToJson(
      aggregateCovariateSettings$covariateSettings
    )
  )

  result$settings <- data.frame(
    runId = runId,
    databaseId = databaseId,
    covariateSettingJson = covariateSettingsJson,
    riskWindowStart = aggregateCovariateSettings$riskWindowStart,
    startAnchor = aggregateCovariateSettings$startAnchor,
    riskWindowEnd = aggregateCovariateSettings$riskWindowEnd,
    endAnchor = aggregateCovariateSettings$endAnchor
  )

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropAggregateCovariate.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql, progressBar = FALSE,
    reportOverallTime = FALSE
  )

  return(result)
}


createCohortsOfInterest <- function(
    connection,
    cdmDatabaseSchema,
    dbms,
    aggregateCovariateSettings,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    tempEmulationSchema) {
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "createTargetOutcomeCombinations.sql",
    packageName = "Characterization",
    dbms = dbms,
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    target_database_schema = targetDatabaseSchema,
    target_table = targetTable,
    outcome_database_schema = outcomeDatabaseSchema,
    outcome_table = outcomeTable,
    target_ids = paste(aggregateCovariateSettings$targetIds, collapse = ",", sep = ","),
    outcome_ids = paste(aggregateCovariateSettings$outcomeIds, collapse = ",", sep = ","),
    min_prior_observation = aggregateCovariateSettings$minPriorObservation,
    tar_start = aggregateCovariateSettings$riskWindowStart,
    tar_start_anchor = ifelse(
      aggregateCovariateSettings$startAnchor == "cohort start",
      "cohort_start_date",
      "cohort_end_date"
    ),
    tar_end = aggregateCovariateSettings$riskWindowEnd,
    tar_end_anchor = ifelse(
      aggregateCovariateSettings$endAnchor == "cohort start",
      "cohort_start_date",
      "cohort_end_date"
    )
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}
