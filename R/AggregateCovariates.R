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
#' @param caseCovariateSettings An object created using \code{createDuringCovariateSettings}
#' @param casePreTargetDuration    The number of days prior to case index we use for FeatureExtraction
#' @param casePostOutcomeDuration    The number of days prior to case index we use for FeatureExtraction
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
      useDemographicsIndexYear = T,
      useDemographicsIndexMonth = T,
      useDemographicsTimeInCohort = T,
      useDemographicsPriorObservationTime = T,
      useDemographicsPostObservationTime = T,
      useConditionGroupEraLongTerm = T,
      useDrugGroupEraOverlapping = T,
      useDrugGroupEraLongTerm = T,
      useProcedureOccurrenceLongTerm = T,
      useMeasurementLongTerm = T,
      useObservationLongTerm = T,
      useDeviceExposureLongTerm = T,
      useVisitConceptCountLongTerm = T,
      useConditionGroupEraShortTerm = T,
      useDrugGroupEraShortTerm = T,
      useProcedureOccurrenceShortTerm = T,
      useMeasurementShortTerm = T,
      useObservationShortTerm = T,
      useDeviceExposureShortTerm = T,
      useVisitConceptCountShortTerm = T,
      endDays = 0,
      longTermStartDays =  -365,
      shortTermStartDays = -30
    ),
    caseCovariateSettings = createDuringCovariateSettings(
      useConditionGroupEraDuring = T,
      useDrugGroupEraDuring = T,
      useProcedureOccurrenceDuring = T,
      useDeviceExposureDuring = T,
      useMeasurementDuring = T,
      useObservationDuring = T,
      useVisitConceptCountDuring = T
      ),
    casePreTargetDuration = 365,
    casePostOutcomeDuration = 365,
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

  # check TAR - EFF edit
  for(i in 1:length(riskWindowStart)){
    .checkTimeAtRisk(
      riskWindowStart = riskWindowStart[i],
      startAnchor = startAnchor[i],
      riskWindowEnd = riskWindowEnd[i],
      endAnchor = endAnchor[i],
      errorMessages = errorMessages
    )
  }

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

  # check unique Ts and Os
  if(length(targetIds) != length(unique(targetIds))){
    message('targetIds have duplicates - making unique')
    targetIds <- unique(targetIds)
  }
  if(length(outcomeIds) != length(unique(outcomeIds))){
    message('outcomeIds have duplicates - making unique')
    outcomeIds <- unique(outcomeIds)
  }


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
    caseCovariateSettings = caseCovariateSettings,
    casePreTargetDuration = casePreTargetDuration,
    casePostOutcomeDuration = casePostOutcomeDuration,
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

  # check cohortDefinitionIds are unique
  if(sum(table(cohortDetails$cohortDefinitionIds)> 1) != 0){
    stop('Unique constrain on cohortDefinitionIds failed')
  }

  # for each time-at-risk get case details
  for(i in 1:length(aggregateCovariateSettings$riskWindowStart)){
    cohortDetails <- rbind(
      cohortDetails,
      getCaseDetails(
        targetIds = aggregateCovariateSettings$targetIds,
        outcomeIds = aggregateCovariateSettings$outcomeIds,
        timeAtRiskId = i
      ))
  }


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

  message("Computing case covariate results")
  resultCase <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_cases",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = aggregateCovariateSettings$caseCovariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
  )

  # add covariates to table
  if(!is.null(result$covariates)){
    if (!is.null(resultCase$covariates)) {
      Andromeda::appendToTable(
        tbl = result$covariates,
        data = resultCase$covariates
      )
    }
  } else{
    if (!is.null(resultCase$covariates)) {
      result$covariates <- resultCase$covariates
    }
  }

  # covariatesContinuous
  if(!is.null(result$covariatesContinuous)){
    if (!is.null(resultCase$covariatesContinuous)) {
      Andromeda::appendToTable(
        tbl = result$covariatesContinuous,
        data = resultCase$covariatesContinuous
      )
    }
  } else{
    if (!is.null(resultCase$covariatesContinuous)) {
      result$covariatesContinuous <- resultCase$covariatesContinuous
    }
  }

  # update covariateRef
  result$covariateRef <- unique(
    rbind(
      as.data.frame(result$covariateRef),
      as.data.frame(resultCase$covariateRef)
    )
  )

  # update analysisRef
  result$analysisRef <- unique(
    rbind(
      as.data.frame(result$analysisRef),
      as.data.frame(resultCase$analysisRef)
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

  caseCovariateSettingsJson <- as.character(
    ParallelLogger::convertSettingsToJson(
      aggregateCovariateSettings$caseCovariateSettings
    )
  )

  result$settings <- data.frame(
    runId = runId,
    databaseId = databaseId,
    covariateSettingJson = covariateSettingsJson,
    caseCovariateSettingsJson = caseCovariateSettingsJson,
    casePostOutcomeDuration = aggregateCovariateSettings$casePostOutcomeDuration,
    casePreTargetDuration = aggregateCovariateSettings$casePreTargetDuration,
    minPriorObservation = aggregateCovariateSettings$minPriorObservation,
    outcomeWashoutDays = aggregateCovariateSettings$outcomeWashoutDays,
    minCharacterizationMean = aggregateCovariateSettings$minCharacterizationMean
  )

  result$timeAtRisk <- data.frame(
    runId = runId,
    databaseId = databaseId,
    timeAtRiskId = 1:length(aggregateCovariateSettings$riskWindowStart),
    riskWindowStart = aggregateCovariateSettings$riskWindowStart,
    riskWindowEnd = aggregateCovariateSettings$riskWindowEnd,
    startAnchor = aggregateCovariateSettings$startAnchor,
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
    tempEmulationSchema
    ) {

  # first create Ts and Os calling RestrictCohorts.sql
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "RestrictCohorts.sql",
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
    outcome_washout_days = aggregateCovariateSettings$outcomeWashoutDays
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )


  # now create the cases per TAR
  for(i in 1:length(aggregateCovariateSettings$riskWindowStart)){
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CreateCases.sql",
      packageName = "Characterization",
      dbms = dbms,
      tempEmulationSchema = tempEmulationSchema,
      tar_start = aggregateCovariateSettings$riskWindowStart[i],
      tar_start_anchor = ifelse(
        aggregateCovariateSettings$startAnchor[i] == "cohort start",
        "cohort_start_date",
        "cohort_end_date"
      ),
      tar_end = aggregateCovariateSettings$riskWindowEnd[i],
      tar_end_anchor = ifelse(
        aggregateCovariateSettings$endAnchor[i] == "cohort start",
        "cohort_start_date",
        "cohort_end_date"
      ),
      time_at_risk_id = i,
      first = i==1,
      case_post_outcome_duration = aggregateCovariateSettings$casePostOutcomeDuration,
      case_pre_target_duration = aggregateCovariateSettings$casePreTargetDuration
    )

    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }

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
    ),
    timeAtRiskId = 0
  )

  comboTypes <- 'TnOprior'
  for(comboType in comboTypes){
    cohortDetailsExtra <- as.data.frame(
      merge(
        targetIds,
        outcomeIds)
    )
    colnames(cohortDetailsExtra) <- c('targetCohortId', 'outcomeCohortId')
    cohortDetailsExtra$cohortType <- comboType
    cohortDetailsExtra$timeAtRiskId <- 0
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

getCaseDetails <- function(
    targetIds,
    outcomeIds,
    timeAtRiskId
){

  cohortDetails <- c()

  comboTypes <- c('TnObetween','OnT', 'TnO')
  for(comboType in comboTypes){
    cohortDetailsExtra <- as.data.frame(
      merge(
        targetIds,
        outcomeIds)
    )
    colnames(cohortDetailsExtra) <- c('targetCohortId', 'outcomeCohortId')
    cohortDetailsExtra$cohortType <- comboType
    cohortDetailsExtra$timeAtRiskId <- timeAtRiskId
    cohortDetails <- rbind(cohortDetails, cohortDetailsExtra)
  }

  cohortDetails$cohortDefinitionId <- apply(
    cohortDetails,
    1,
    function(x){
      hashInt(T = x[1], O = x[2], type = paste0(x[3], timeAtRiskId))
    }
  )
  return(cohortDetails)
}


