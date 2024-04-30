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
#' @param minPriorObservation The minimum time (in days) in the database a patient in the target cohorts must be observed prior to index
#' @param outcomeWashoutDays Patients with the outcome within outcomeWashout days prior to index are excluded from the risk factor analysis
#' @template timeAtRisk
#' @param covariateSettings   An object created using \code{FeatureExtraction::createCovariateSettings}
#' @param duringCovariateSettings An object created using \code{createDuringCovariateSettings}
#' @param afterCovariateSettings An object created using \code{FeatureExtraction::createCovariateSettings}
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
    outcomeWashoutDays = 0,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    covariateSettings = FeatureExtraction::createCovariateSettings(
      useDemographicsGender = T,
      useDemographicsAge = T,
      useDemographicsAgeGroup = T,
      useDemographicsRace = T,
      useDemographicsEthnicity = T,
      useDemographicsTimeInCohort = T,
      useDemographicsPriorObservationTime = T,
      useDemographicsPostObservationTime = T,
      useConditionGroupEraLongTerm = T,
      useDrugGroupEraLongTerm = T,
      useProcedureOccurrenceLongTerm = T,
      useMeasurementLongTerm = T,
      useObservationLongTerm = T,
      useDeviceExposureLongTerm = T,
      useVisitConceptCountLongTerm = T,
      endDays = -1,
      longTermStartDays =  -365
    ),
    duringCovariateSettings = createDuringCovariateSettings(
      useConditionGroupEraDuring = T,
      useDrugGroupEraDuring = T,
      useProcedureOccurrenceDuring = T,
      useDeviceExposureDuring = T,
      useMeasurementDuring = T,
      useObservationDuring = T,
      useVisitConceptCountDuring = T
      ),
    afterCovariateSettings = FeatureExtraction::createCovariateSettings(
      useConditionGroupEraMediumTerm = T,
      useDrugGroupEraMediumTerm = T,
      useProcedureOccurrenceMediumTerm = T,
      useMeasurementMediumTerm = T,
      useObservationMediumTerm = T,
      useDeviceExposureMediumTerm = T,
      useVisitConceptCountMediumTerm = T,
      endDays = 365,
      mediumTermStartDays = 1,
      ),
    minCharacterizationMean = 0
    ) {
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

  # check temporal is false
  if(inherits(covariateSettings, 'covariateSettings')){
    covariateSettings <- list(covariateSettings)
  }
  if(sum(unlist(lapply(covariateSettings, function(x){x$temporal})))>0){
    stop('Temporal covariateSettings not supported by createAggregateCovariateSettings()')
  }

  # check minPriorObservation
  .checkMinPriorObservation(
    minPriorObservation = minPriorObservation,
    errorMessages = errorMessages
  )

  # add check for outcomeWashoutDays

  checkmate::reportAssertions(errorMessages)

  # create list
  result <- list(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    minPriorObservation = minPriorObservation,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    covariateSettings = covariateSettings,
    duringCovariateSettings = duringCovariateSettings,
    afterCovariateSettings = afterCovariateSettings,
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


  # creating cohort_details
  message("Creating and inserting cohort details")
  cohortDetails <- getCohortDetails(
    targetIds = aggregateCovariateSettings$targetIds,
    outcomeIds = aggregateCovariateSettings$outcomeIds
    )
  DatabaseConnector::insertTable(
    data = cohortDetails,
    camelCaseToSnakeCase = T,
    connection = connection,
    tableName =  '#cohort_details',
    tempTable = T,
    dropTableIfExists = T,
    createTable = T,
    progressBar = T
    )

  # select T, O, create TnO, TnOc, Onprior T
  # into temp table #agg_cohorts
  message("Computing aggregate covariate cohorts")
  start <- Sys.time()
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
  completionTime <- Sys.time() - start
  message(paste0('Computing aggregate covariate cohorts took ',round(completionTime,digits = 1), ' ', units(completionTime)))
  ## get counts
  message("Extracting cohort counts")
  sql <- "select
  cohort_definition_id,
  count(*) row_count,
  count(distinct subject_id) person_count,
  min(datediff(day, cohort_start_date, cohort_end_date)) min_exposure_time,
  avg(datediff(day, cohort_start_date, cohort_end_date)) mean_exposure_time,
  max(datediff(day, cohort_start_date, cohort_end_date)) max_exposure_time
  from
  (select * from #agg_cohorts_before union select * from #agg_cohorts_extras) temp
  group by cohort_definition_id;"
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connectionDetails$dbms
  )
  counts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = T,
  )

  message("Computing aggregate before covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_before",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = aggregateCovariateSettings$covariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
  )

  message("Computing aggregate between covariate results")
  resultBetween <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_between",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = aggregateCovariateSettings$duringCovariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
  )

  # add covariates to table
  if(!is.null(result$covariates)){
    if (!is.null(resultBetween$covariates)) {
      Andromeda::appendToTable(
        tbl = result$covariates,
        data = resultBetween$covariates
      )
    }
  } else{
    if (!is.null(resultBetween$covariates)) {
      result$covariates <- resultBetween$covariates
    }
  }

  # covariatesContinuous
  if(!is.null(result$covariatesContinuous)){
    if (!is.null(resultBetween$covariatesContinuous)) {
      Andromeda::appendToTable(
        tbl = result$covariatesContinuous,
        data = resultBetween$covariatesContinuous
      )
    }
  } else{
    if (!is.null(resultBetween$covariatesContinuous)) {
      result$covariatesContinuous <- resultBetween$covariatesContinuous
    }
  }

  # update covariateRef
  result$covariateRef <- unique(
    rbind(
      as.data.frame(result$covariateRef),
      as.data.frame(resultBetween$covariateRef)
    )
  )

  # after
  message("Computing aggregate after covariate results")
  resultAfter <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_after",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = aggregateCovariateSettings$afterCovariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
  )

  if(!is.null(result$covariates)){
    if (!is.null(resultAfter$covariates)) {
      Andromeda::appendToTable(
        tbl = result$covariates,
        data = resultAfter$covariates
      )
    }
  } else{
    if (!is.null(resultAfter$covariates)) {
      result$covariates <- resultAfter$covariates
    }
  }

  # covariatesContinuous
  if(!is.null(result$covariatesContinuous)){
    if (!is.null(resultAfter$covariatesContinuous)) {
      Andromeda::appendToTable(
        tbl = result$covariatesContinuous,
        data = resultAfter$covariatesContinuous
      )
    }
  } else{
    if (!is.null(resultAfter$covariatesContinuous)) {
      result$covariatesContinuous <- resultAfter$covariatesContinuous
    }
  }


  # update covariateRef
  result$covariateRef <- unique(
    rbind(
      as.data.frame(result$covariateRef),
      as.data.frame(resultAfter$covariateRef)
    )
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

  duringCovariateSettingsJson <- as.character(
    ParallelLogger::convertSettingsToJson(
      aggregateCovariateSettings$duringCovariateSettings
    )
  )
  afterCovariateSettingsJson <- as.character(
    ParallelLogger::convertSettingsToJson(
      aggregateCovariateSettings$afterCovariateSettings
    )
  )

  result$settings <- data.frame(
    runId = runId,
    databaseId = databaseId,
    covariateSettingJson = covariateSettingsJson,
    duringCovariateSettingsJson = duringCovariateSettingsJson,
    afterCovariateSettingsJson = afterCovariateSettingsJson,
    riskWindowStart = aggregateCovariateSettings$riskWindowStart,
    startAnchor = aggregateCovariateSettings$startAnchor,
    riskWindowEnd = aggregateCovariateSettings$riskWindowEnd,
    endAnchor = aggregateCovariateSettings$endAnchor,
    minPriorObservation = aggregateCovariateSettings$minPriorObservation,
    outcomeWashoutDays = aggregateCovariateSettings$outcomeWashoutDays,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
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
    outcome_washout_days = aggregateCovariateSettings$outcomeWashoutDays,
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

hex_to_int = function(h) {
  xx = strsplit(tolower(h), "")[[1L]]
  pos = match(xx, c(0L:9L, letters[1L:6L]))
  sum((pos - 1L) * 16^(rev(seq_along(xx) - 1)))
}

hashInt <- function(T, O, type){
  a <- digest::digest(
    object = paste0(T,O,type),
    algo='xxhash32',
    seed=0
    )
  result <- hex_to_int(a)
  return(result)
}

getCohortDetails <- function(
    targetIds,
    outcomeIds
){
  cohortDetails <- data.frame(
    targetCohortId = c(rep(targetIds, 2), rep(0,2*length(outcomeIds))),
    outcomeCohortId = c(rep(0,2*length(targetIds)), rep(outcomeIds, 2)),
    cohortType = c(
      rep('Tall', length(targetIds)),
      rep('T', length(targetIds)),
      rep('Oall', length(outcomeIds)),
      rep('O', length(outcomeIds))
    )
  )
  comboTypes <- c('TnObetween','OnT', 'TnOprior', 'TnO')
  for(comboType in comboTypes){
    cohortDetailsExtra <- as.data.frame(
      merge(
        targetIds,
        outcomeIds)
    )
    colnames(cohortDetailsExtra) <- c('targetCohortId', 'outcomeCohortId')
    cohortDetailsExtra$cohortType <- comboType
    cohortDetails <- rbind(cohortDetails, cohortDetailsExtra)
  }

  cohortDetails$cohortDefinitionId <- apply(
    cohortDetails,
    1,
    function(x){
      hashInt(T = x[1], O = x[2], type = x[3])
    }
  )
  return(cohortDetails)
}
