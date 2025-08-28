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
#' @param extractNonCaseCovariates Whether to extract aggregate covariates and counts for patients in the targets and outcomes in addition to the cases
#' @family Aggregate
#' @return
#' A list with the settings
#'
#' @examples
#'
#' aggregateSetting <- createAggregateCovariateSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = c(3),
#'   minPriorObservation = 365,
#'   outcomeWashoutDays = 90,
#'   riskWindowStart = 1,
#'   startAnchor = "cohort start",
#'   riskWindowEnd = 365,
#'   endAnchor = "cohort start",
#'   casePreTargetDuration = 365,
#'   casePostOutcomeDuration = 365
#' )
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
      useDemographicsGender = TRUE,
      useDemographicsAge = TRUE,
      useDemographicsAgeGroup = TRUE,
      useDemographicsRace = TRUE,
      useDemographicsEthnicity = TRUE,
      useDemographicsIndexYear = TRUE,
      useDemographicsIndexMonth = TRUE,
      useDemographicsTimeInCohort = TRUE,
      useDemographicsPriorObservationTime = TRUE,
      useDemographicsPostObservationTime = TRUE,
      useConditionGroupEraLongTerm = TRUE,
      useDrugGroupEraOverlapping = TRUE,
      useDrugGroupEraLongTerm = TRUE,
      useProcedureOccurrenceLongTerm = TRUE,
      useMeasurementLongTerm = TRUE,
      useObservationLongTerm = TRUE,
      useDeviceExposureLongTerm = TRUE,
      useVisitConceptCountLongTerm = TRUE,
      useConditionGroupEraShortTerm = TRUE,
      useDrugGroupEraShortTerm = TRUE,
      useProcedureOccurrenceShortTerm = TRUE,
      useMeasurementShortTerm = TRUE,
      useObservationShortTerm = TRUE,
      useDeviceExposureShortTerm = TRUE,
      useVisitConceptCountShortTerm = TRUE,
      endDays = 0,
      longTermStartDays = -365,
      shortTermStartDays = -30
    ),
    caseCovariateSettings = createDuringCovariateSettings(
      useConditionGroupEraDuring = TRUE,
      useDrugGroupEraDuring = TRUE,
      useProcedureOccurrenceDuring = TRUE,
      useDeviceExposureDuring = TRUE,
      useMeasurementDuring = TRUE,
      useObservationDuring = TRUE,
      useVisitConceptCountDuring = TRUE
    ),
    casePreTargetDuration = 365,
    casePostOutcomeDuration = 365,
    extractNonCaseCovariates = TRUE) {
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
  if (length(riskWindowStart) > 1) {
    stop("Please add one time-at-risk per setting")
  }
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
  if (inherits(covariateSettings, "covariateSettings")) {
    covariateSettings <- list(covariateSettings)
  }
  if (sum(unlist(lapply(covariateSettings, function(x) {
    x$temporal
  }))) > 0) {
    stop("Temporal covariateSettings not supported by createAggregateCovariateSettings()")
  }

  # check minPriorObservation
  .checkMinPriorObservation(
    minPriorObservation = minPriorObservation,
    errorMessages = errorMessages
  )

  # add check for outcomeWashoutDays

  checkmate::reportAssertions(errorMessages)

  # check unique Ts and Os
  if (length(targetIds) != length(unique(targetIds))) {
    message("targetIds have duplicates - making unique")
    targetIds <- unique(targetIds)
  }
  if (length(outcomeIds) != length(unique(outcomeIds))) {
    message("outcomeIds have duplicates - making unique")
    outcomeIds <- unique(outcomeIds)
  }


  # create list
  result <- list(
    targetIds = targetIds,
    minPriorObservation = minPriorObservation,
    outcomeIds = outcomeIds,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    covariateSettings = covariateSettings, # risk factors
    caseCovariateSettings = caseCovariateSettings, # case series
    casePreTargetDuration = casePreTargetDuration, # case series
    casePostOutcomeDuration = casePostOutcomeDuration, # case series,

    extractNonCaseCovariates = extractNonCaseCovariates
  )

  class(result) <- "aggregateCovariateSettings"
  return(result)
}

createExecutionIds <- function(size) {
  executionIds <- gsub(" ", "", gsub("[[:punct:]]", "", paste(Sys.time(), sample(1000000, size), sep = "")))
  return(executionIds)
}

# TODO cdmVersion should be in runChar
computeTargetAggregateCovariateAnalyses <- function(
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cdmVersion = 5,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema, # remove
    outcomeTable = targetTable, # remove
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder,
    minCharacterizationMean = 0,
    minCellCount = 0,
    progressBar = interactive(),
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  message("Target Aggregate:  starting")

  # get settings
  settingId <- unique(settings$settingId)
  targetIds <- unique(settings$targetId)
  minPriorObservation <- settings$minPriorObservation
  covariateSettings <- settings$covariateSettings

  # create cohortDetails - all Ts, minPriorObservation, twice (type = Tall, Target)
  cohortDetails <- data.frame(
    settingId = settingId,
    targetCohortId = rep(targetIds, 2),
    outcomeCohortId = 0, # cannot be NA due to pk/index
    cohortType = c(rep("Target", length(targetIds)), rep("Tall", length(targetIds))),
    cohortDefinitionId = 1:(length(targetIds) * 2),
    minPriorObservation = minPriorObservation,
    outcomeWashoutDays = NA,
    casePreTargetDuration = NA,
    casePostOutcomeDuration = NA,
    riskWindowStart = NA,
    startAnchor = NA,
    riskWindowEnd = NA,
    endAnchor = NA,
    covariateSettingJson = covariateSettings,
    caseCovariateSettingJson = NA
  )

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # create the temp table with cohort_details
  DatabaseConnector::insertTable(
    data = cohortDetails[, c("settingId", "targetCohortId", "outcomeCohortId", "cohortType", "cohortDefinitionId")],
    camelCaseToSnakeCase = TRUE,
    connection = connection,
    tableName = "#cohort_details",
    tempTable = TRUE,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    progressBar = progressBar,
    tempEmulationSchema = tempEmulationSchema
  )

  message("Target Aggregate: Computing aggregate target cohorts")
  start <- Sys.time()

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "TargetCohorts.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    target_database_schema = targetDatabaseSchema,
    target_table = targetTable,
    target_ids = paste(targetIds, collapse = ",", sep = ","),
    min_prior_observation = minPriorObservation
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )
  completionTime <- Sys.time() - start

  message(paste0("Target Aggregate: Computing target cohorts took ", round(completionTime, digits = 1), " ", units(completionTime)))
  ## get counts
  message("Extracting target cohort counts")
  sql <- "select
  cohort_definition_id,
  count_big(*) row_count,
  count(distinct subject_id) person_count,
  min(cast(datediff(day, cohort_start_date, cohort_end_date) as bigint)) min_exposure_time,
  avg(cast(datediff(day, cohort_start_date, cohort_end_date) as bigint)) mean_exposure_time,
  max(cast(datediff(day, cohort_start_date, cohort_end_date) as bigint)) max_exposure_time
  from
  (select * from #agg_cohorts_before union select * from #agg_cohorts_extras) temp
  group by cohort_definition_id;"
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  counts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE
  )

  message("Target Aggregate: Computing aggregate target covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_before",
    cohortTableIsTemp = TRUE,
    cohortIds = -1,
    covariateSettings = ParallelLogger::convertJsonToSettings(covariateSettings),
    cdmVersion = cdmVersion,
    aggregated = TRUE,
    minCharacterizationMean = minCharacterizationMean,
    tempEmulationSchema = tempEmulationSchema
  )

  # drop temp tables
  message("Target Aggregate: Dropping temp tables")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropTargetCovariate.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )

  # export all results to csv files
  message("Target Aggregate: Exporting to csv")
  exportAndromedaToCsv(
    andromeda = result,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = counts,
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean,
    minCellCount = minCellCount
  )

  message("Target Aggregate:  ending")

  return(invisible(TRUE))
}


computeCaseAggregateCovariateAnalyses <- function(
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cdmVersion = 5,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema, # remove
    outcomeTable = targetTable, # remove
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder,
    minCharacterizationMean = 0,
    minCellCount = 0,
    progressBar = interactive(),
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  message("Case Aggregates:  starting")
  # check inputs

  # create cohortDetails - all Ts, minPriorObservation, twice (type = Tall, Target)


  # get settings
  targetIds <- unique(settings$targetId)
  outcomeIds <- unique(settings$outcomeId)
  minPriorObservation <- settings$minPriorObservation
  outcomeWashoutDays <- settings$outcomeWashoutDays
  casePreTargetDuration <- settings$casePreTargetDuration
  casePostOutcomeDuration <- settings$casePostOutcomeDuration
  covariateSettings <- settings$covariateSettings # json
  caseCovariateSettings <- settings$caseCovariateSettings # json

  # 'cohortType'
  cohortDetails <- expand.grid(
    targetCohortId = unique(settings$targetId),
    outcomeCohortId = unique(settings$outcomeId),
    cohortType = c("Cases", "CasesBefore", "CasesAfter", "CasesBetween")
  )

  cohortDetails$minPriorObservation <- settings$minPriorObservation
  cohortDetails$outcomeWashoutDays <- settings$outcomeWashoutDays
  cohortDetails$casePreTargetDuration <- settings$casePreTargetDuration
  cohortDetails$casePostOutcomeDuration <- settings$casePostOutcomeDuration
  cohortDetails$covariateSettingJson <- settings$covariateSettings
  cohortDetails$caseCovariateSettingJson <- settings$caseCovariateSettings

  tars <- settings$tar
  tars$settingId <- unique(settings$settingId)

  # add executionIds
  cohortDetails <- merge(cohortDetails, tars)
  # moved id so different tars have different id?
  cohortDetails$cohortDefinitionId <- 1:nrow(cohortDetails)

  # add 'Exclude' with random tar
  cohortDetailsExtra <- expand.grid(
    targetCohortId = unique(settings$targetId),
    outcomeCohortId = unique(settings$outcomeId),
    cohortType = "Exclude",
    minPriorObservation = settings$minPriorObservation,
    outcomeWashoutDays = settings$outcomeWashoutDays,
    casePreTargetDuration = settings$casePreTargetDuration,
    casePostOutcomeDuration = settings$casePostOutcomeDuration,
    covariateSettingJson = settings$covariateSettings,
    caseCovariateSettingJson = settings$caseCovariateSettings,
    riskWindowStart = tars$riskWindowStart[1],
    startAnchor = tars$startAnchor[1],
    riskWindowEnd = tars$riskWindowEnd[1],
    endAnchor = tars$endAnchor[1],
    settingId = settings$settingId[1]
  )
  cohortDetailsExtra$cohortDefinitionId <- max(cohortDetails$cohortDefinitionId) + (1:nrow(cohortDetailsExtra))
  cohortDetails <- rbind(cohortDetails, cohortDetailsExtra[colnames(cohortDetails)])

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # create the temp table with cohort_details
  DatabaseConnector::insertTable(
    data = cohortDetails[, c("targetCohortId", "outcomeCohortId", "cohortType", "cohortDefinitionId", "settingId")],
    camelCaseToSnakeCase = TRUE,
    connection = connection,
    tableName = "#cohort_details",
    tempTable = TRUE,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    progressBar = progressBar,
    tempEmulationSchema = tempEmulationSchema
  )

  message("Case Aggregates: Computing aggregate case covariate cohorts")
  start <- Sys.time()

  # this is run for all tars
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CaseCohortsPart1.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    target_database_schema = targetDatabaseSchema,
    target_table = targetTable,
    target_ids = paste(targetIds, collapse = ",", sep = ","),
    outcome_database_schema = outcomeDatabaseSchema,
    outcome_table = outcomeTable,
    outcome_ids = paste(outcomeIds, collapse = ",", sep = ","),
    min_prior_observation = minPriorObservation,
    outcome_washout_days = outcomeWashoutDays
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )

  # extract the excluded people into excluded_covariates, excluded_covariates_continuous,
  # excluded_analysis_ref, excluded_covariate_ref

  # loop over settingId which contains tars:
  for (i in 1:nrow(tars)) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CaseCohortsPart2.sql",
      packageName = "Characterization",
      dbms = connectionDetails$dbms,
      tempEmulationSchema = tempEmulationSchema,
      first = i == 1,
      case_pre_target_duration = casePreTargetDuration,
      case_post_outcome_duration = casePostOutcomeDuration,
      setting_id = tars$settingId[i],
      tar_start = tars$riskWindowStart[i],
      tar_start_anchor = ifelse(tars$startAnchor[i] == "cohort start", "cohort_start_date", "cohort_end_date"), ## TODO change?
      tar_end = tars$riskWindowEnd[i],
      tar_end_anchor = ifelse(tars$endAnchor[i] == "cohort start", "cohort_start_date", "cohort_end_date") ## TODO change?
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = progressBar,
      reportOverallTime = FALSE
    )
  }
  completionTime <- Sys.time() - start

  message(paste0("Case Aggregates:  Computing case cohorts took ", round(completionTime, digits = 1), " ", units(completionTime)))

  ## get counts
  message("Case Aggregates:  Extracting case cohort counts")
  sql <- "select
  cohort_definition_id,
  count(*) row_count,
  count(distinct subject_id) person_count,
  min(datediff(day, cohort_start_date, cohort_end_date)) min_exposure_time,
  avg(datediff(day, cohort_start_date, cohort_end_date)) mean_exposure_time,
  max(datediff(day, cohort_start_date, cohort_end_date)) max_exposure_time
  from #cases
  group by cohort_definition_id;"
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  counts <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE
  )

  message("Case Aggregates: Computing aggregate before case covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#cases",
    cohortTableIsTemp = TRUE,
    cohortIds = -1,
    covariateSettings = ParallelLogger::convertJsonToSettings(covariateSettings),
    cdmVersion = cdmVersion,
    aggregated = TRUE,
    minCharacterizationMean = minCharacterizationMean,
    tempEmulationSchema = tempEmulationSchema
  )

  message("Case Aggregates:  Computing aggregate during case covariate results")

  result2 <- tryCatch(
    {
      FeatureExtraction::getDbCovariateData(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortTable = "#case_series",
        cohortTableIsTemp = TRUE,
        cohortIds = -1,
        covariateSettings = ParallelLogger::convertJsonToSettings(caseCovariateSettings),
        cdmVersion = cdmVersion,
        aggregated = TRUE,
        minCharacterizationMean = minCharacterizationMean,
        tempEmulationSchema = tempEmulationSchema
      )
    },
    error = function(e) {
      message(e)
      return(NULL)
    }
  )
  if (is.null(result2)) {
    stop("Issue with case series data extraction")
  }

  # drop temp tables
  message("Case Aggregates: Dropping temp tables")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropCaseCovariate.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )

  # export all results to csv files
  message("Case Aggregates:  Exporting results to csv")
  exportAndromedaToCsv( # TODO combine export of result and result2
    andromeda = result,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = counts,
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean,
    minCellCount = minCellCount
  )
  exportAndromedaToCsv(
    andromeda = result2,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = NULL, # previously added
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean,
    includeSettings = FALSE,
    minCellCount = minCellCount
  )

  message("Case Aggregates:  ending")

  return(invisible(TRUE))
}


exportAndromedaToCsv <- function(
    andromeda,
    outputFolder,
    cohortDetails,
    counts,
    databaseId,
    minCharacterizationMean,
    batchSize = 100000,
    minCellCount = 0,
    includeSettings = TRUE
    ) {
  saveLocation <- outputFolder
  if (!dir.exists(saveLocation)) {
    dir.create(saveLocation, recursive = T)
  }

  ids <- data.frame(
    settingId = unique(cohortDetails$settingId),
    databaseId = databaseId
  )

  # analysis_ref and covariate_ref
  # add database_id and setting_id
  if (!is.null(andromeda$analysisRef)) {
    Andromeda::batchApply(
      tbl = andromeda$analysisRef,
      fun = function(x) {
        data <- merge(x, ids)
        colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

        if (file.exists(file.path(saveLocation, "analysis_ref.csv"))) {
          append <- TRUE
        } else {
          append <- FALSE
        }
        readr::write_csv(
          x = formatDouble(data),
          file = file.path(saveLocation, "analysis_ref.csv"),
          append = append
        )
      },
      batchSize = batchSize
    )
  }

  if (!is.null(andromeda$covariateRef)) {
    Andromeda::batchApply(
      tbl = andromeda$covariateRef,
      fun = function(x) {
        data <- merge(x, ids)
        colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

        if (file.exists(file.path(saveLocation, "covariate_ref.csv"))) {
          append <- TRUE
        } else {
          append <- FALSE
        }
        readr::write_csv(
          x = formatDouble(data),
          file = file.path(saveLocation, "covariate_ref.csv"),
          append = append
        )
      },
      batchSize = batchSize
    )
  }

  # covariates and covariate_continuous
  extras <- cohortDetails[, c("cohortDefinitionId", "settingId", "targetCohortId", "outcomeCohortId", "cohortType")]
  extras$databaseId <- databaseId
  extras$minCharacterizationMean <- minCharacterizationMean

  # add database_id, setting_id, target_cohort_id, outcome_cohort_id and cohort_type
  if (!is.null(andromeda$covariates)) {
    Andromeda::batchApply(
      tbl = andromeda$covariates,
      fun = function(x) {
        data <- merge(x, extras, by = "cohortDefinitionId")
        data <- data %>% dplyr::select(-"cohortDefinitionId")
        colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

        # censor minCellCount columns sum_value
        removeInd <- data$sum_value < minCellCount
        if (sum(removeInd) > 0) {
          ParallelLogger::logInfo(paste0("Removing sum_value counts less than ", minCellCount))
          if (sum(removeInd) > 0) {
            data$sum_value[removeInd] <- -1 * minCellCount
            # adding other calculated columns
            data$average_value[removeInd] <- NA
          }
        }

        if (file.exists(file.path(saveLocation, "covariates.csv"))) {
          append <- TRUE
        } else {
          append <- FALSE
        }
        readr::write_csv(
          x = formatDouble(data),
          file = file.path(saveLocation, "covariates.csv"),
          append = append
        )
      },
      batchSize = batchSize
    )
  }

  if (!is.null(andromeda$covariatesContinuous)) {
    Andromeda::batchApply(
      tbl = andromeda$covariatesContinuous,
      fun = function(x) {
        data <- merge(x, extras %>% dplyr::select(-"minCharacterizationMean"), by = "cohortDefinitionId")
        data <- data %>% dplyr::select(-"cohortDefinitionId")
        colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

        # count_value
        removeInd <- data$count_value < minCellCount
        if (sum(removeInd) > 0) {
          ParallelLogger::logInfo(paste0("Removing count_value counts less than ", minCellCount))
          if (sum(removeInd) > 0) {
            data$count_value[removeInd] <- -1 * minCellCount
            # adding columns calculated from count
            data$min_value[removeInd] <- NA
            data$max_value[removeInd] <- NA
            data$average_value[removeInd] <- NA
            data$standard_deviation[removeInd] <- NA
            data$median_value[removeInd] <- NA
            data$p_10_value[removeInd] <- NA
            data$p_25_value[removeInd] <- NA
            data$p_75_value[removeInd] <- NA
            data$p_90_value[removeInd] <- NA
          }
        }

        if (file.exists(file.path(saveLocation, "covariates_continuous.csv"))) {
          append <- TRUE
        } else {
          append <- FALSE
        }
        readr::write_csv(
          x = formatDouble(data),
          file = file.path(saveLocation, "covariates_continuous.csv"),
          append = append
        )
      },
      batchSize = batchSize
    )
  }

  # cohort_counts:
  if (!is.null(counts)) {
    cohortCounts <- cohortDetails %>%
      dplyr::select(
        "targetCohortId",
        "outcomeCohortId",
        "cohortType",
        "cohortDefinitionId",
        "riskWindowStart",
        "riskWindowEnd",
        "startAnchor",
        "endAnchor",
        "minPriorObservation",
        "outcomeWashoutDays"
      ) %>%
      dplyr::mutate(
        databaseId = !!databaseId
      ) %>%
      dplyr::inner_join(counts, by = "cohortDefinitionId") %>%
      dplyr::select(-"cohortDefinitionId")
    cohortCounts <- unique(cohortCounts)
    colnames(cohortCounts) <- SqlRender::camelCaseToSnakeCase(colnames(cohortCounts))

    # TODO apply minCellCount to columns row_count, person_count
    removeInd <- cohortCounts$row_count < minCellCount
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing row_count counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        cohortCounts$row_count[removeInd] <- -1 * minCellCount
      }
    }
    removeInd <- cohortCounts$person_count < minCellCount
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing person_count counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        cohortCounts$person_count[removeInd] <- -1 * minCellCount
      }
    }

    if (file.exists(file.path(saveLocation, "cohort_counts.csv"))) {
      append <- TRUE
    } else {
      append <- FALSE
    }
    readr::write_csv(
      x = formatDouble(cohortCounts),
      file = file.path(saveLocation, "cohort_counts.csv"),
      append = append
    )
  }

  if (includeSettings) {
    settings <- cohortDetails %>%
      dplyr::select(
        "settingId", "minPriorObservation", "outcomeWashoutDays",
        "riskWindowStart", "riskWindowEnd", "startAnchor", "endAnchor",
        "casePreTargetDuration", "casePostOutcomeDuration",
        "covariateSettingJson", "caseCovariateSettingJson"
      ) %>%
      dplyr::mutate(databaseId = !!databaseId) %>%
      dplyr::distinct()
    colnames(settings) <- SqlRender::camelCaseToSnakeCase(colnames(settings))

    # add setting.csv with cohortDetails plus database
    readr::write_csv(
      x = settings,
      file = file.path(saveLocation, "settings.csv"),
      append = FALSE
    )

    cohortDetails <- cohortDetails %>%
      dplyr::select(
        "settingId", "targetCohortId",
        "outcomeCohortId", "cohortType"
      ) %>%
      dplyr::mutate(databaseId = !!databaseId) %>%
      dplyr::distinct()
    colnames(cohortDetails) <- SqlRender::camelCaseToSnakeCase(colnames(cohortDetails))

    # add cohort_details.csv with cohortDetails plus database
    readr::write_csv(
      x = cohortDetails,
      file = file.path(saveLocation, "cohort_details.csv"),
      append = FALSE
    )
  }

  return(invisible(TRUE))
}




combineCovariateSettingsJsons <- function(covariateSettingsJsonList) {
  # get unique
  covariateSettingsJsonList <- unique(covariateSettingsJsonList)

  # first convert from json
  covariateSettings <- lapply(
    X = covariateSettingsJsonList,
    FUN = function(x) {
      ParallelLogger::convertJsonToSettings(x)
    }
  )

  # then combine the covariates
  singleSettings <- which(unlist(lapply(covariateSettings, function(x) inherits(x, "covariateSettings"))))
  multipleSettings <- which(unlist(lapply(covariateSettings, function(x) inherits(x, "list"))))

  covariateSettingList <- list()
  if (length(singleSettings) > 0) {
    for (i in singleSettings) {
      covariateSettingList[[length(covariateSettingList) + 1]] <- covariateSettings[[i]]
    }
  }
  if (length(multipleSettings) > 0) {
    for (i in multipleSettings) {
      settingList <- covariateSettings[[i]]
      for (j in 1:length(settingList)) {
        if (inherits(settingList[[j]], "covariateSettings")) {
          covariateSettingList[[length(covariateSettingList) + 1]] <- settingList[[j]]
        } else {
          message("Incorrect covariate settings found") # stop?
        }
      }
    }
  }

  # check for covariates with same id but different
  endDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$endDays
  })))
  if (length(endDays) > 1) {
    stop("Covariate settings for aggregate covariates using different end days")
  }
  longTermStartDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$longTermStartDays
  })))
  if (length(longTermStartDays) > 1) {
    stop("Covariate settings for aggregate covariates using different longTermStartDays")
  }
  mediumTermStartDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$mediumTermStartDays
  })))
  if (length(mediumTermStartDays) > 1) {
    stop("Covariate settings for aggregate covariates using different mediumTermStartDays")
  }
  shortTermStartDays <- unique(unlist(lapply(covariateSettingList, function(x) {
    x$shortTermStartDays
  })))
  if (length(shortTermStartDays) > 1) {
    stop("Covariate settings for aggregate covariates using different shortTermStartDays")
  }

  # convert to json
  covariateSettingList <- as.character(ParallelLogger::convertSettingsToJson(covariateSettingList))
  return(covariateSettingList)
}

getAggregateCovariatesJobs <- function(
    characterizationSettings,
    threads) {
  characterizationSettings <- characterizationSettings$aggregateCovariateSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }
  ind <- 1:length(characterizationSettings)


  # target combinations
  targetCombinations <- do.call(
    what = "rbind",
    args =
      lapply(
        1:length(characterizationSettings),
        function(i) {
          if (characterizationSettings[[i]]$extractNonCaseCovariates) {
            result <- data.frame(
              targetIds = c(
                characterizationSettings[[i]]$targetIds,
                characterizationSettings[[i]]$outcomeIds
              ),
              minPriorObservation = characterizationSettings[[i]]$minPriorObservation,
              covariateSettingsJson = as.character(ParallelLogger::convertSettingsToJson(characterizationSettings[[i]]$covariateSettings))
            )
            return(result)
          } else {
            return(
              data.frame(targetIds = 1, minPriorObservation = 1, covariateSettingsJson = 1)[-1, ]
            )
          }
        }
      )
  )

  if (nrow(targetCombinations) > 0) {
    threadCols <- c("targetIds")
    settingCols <- c("minPriorObservation")

    # thread split
    threadSettings <- targetCombinations %>%
      dplyr::select(dplyr::all_of(threadCols)) %>%
      dplyr::distinct()
    threadSettings$thread <- rep(1:threads, ceiling(nrow(threadSettings) / threads))[1:nrow(threadSettings)]
    targetCombinations <- merge(targetCombinations, threadSettings, by = threadCols)

    executionSettings <- data.frame(
      minPriorObservation = unique(targetCombinations$minPriorObservation)
    )
    executionSettings$settingId <- createExecutionIds(nrow(executionSettings))
    targetCombinations <- merge(targetCombinations, executionSettings, by = settingCols)

    # recreate settings
    settings <- c()
    for (settingId in unique(executionSettings$settingId)) {
      settingVal <- executionSettings %>%
        dplyr::filter(.data$settingId == !!settingId) %>%
        dplyr::select(dplyr::all_of(settingCols))

      restrictedData <- targetCombinations %>%
        dplyr::inner_join(settingVal, by = settingCols)

      for (i in unique(restrictedData$thread)) {
        ind <- restrictedData$thread == i
        settings <- rbind(
          settings,
          data.frame(
            functionName = "computeTargetAggregateCovariateAnalyses",
            settings = as.character(ParallelLogger::convertSettingsToJson(
              list(
                targetIds = unique(restrictedData$targetId[ind]),
                minPriorObservation = unique(restrictedData$minPriorObservation[ind]),
                covariateSettingsJson = combineCovariateSettingsJsons(as.list(restrictedData$covariateSettingsJson[ind])),
                settingId = settingId
              )
            )),
            executionFolder = paste("tac", i, paste(settingVal, collapse = "_"), sep = "_"),
            jobId = paste("tac", i, paste(settingVal, collapse = "_"), sep = "_")
          )
        )
      }
    }
  } else {
    settings <- c()
  }

  # adding a delay so the target and case setting ids are always unique
  Sys.sleep(time = 2)

  # get all combinations of TnOs, then split by treads
  caseCombinations <- do.call(
    what = "rbind",
    args =
      lapply(
        1:length(characterizationSettings),
        function(i) {
          result <- expand.grid(
            targetId = characterizationSettings[[i]]$targetIds,
            outcomeId = characterizationSettings[[i]]$outcomeIds
          )
          result$minPriorObservation <- characterizationSettings[[i]]$minPriorObservation
          result$outcomeWashoutDays <- characterizationSettings[[i]]$outcomeWashoutDays

          result$riskWindowStart <- characterizationSettings[[i]]$riskWindowStart
          result$startAnchor <- characterizationSettings[[i]]$startAnchor
          result$riskWindowEnd <- characterizationSettings[[i]]$riskWindowEnd
          result$endAnchor <- characterizationSettings[[i]]$endAnchor

          result$casePreTargetDuration <- characterizationSettings[[i]]$casePreTargetDuration
          result$casePostOutcomeDuration <- characterizationSettings[[i]]$casePostOutcomeDuration

          result$covariateSettingsJson <- as.character(ParallelLogger::convertSettingsToJson(characterizationSettings[[i]]$covariateSettings))
          result$caseCovariateSettingsJson <- as.character(ParallelLogger::convertSettingsToJson(characterizationSettings[[i]]$caseCovariateSettings))
          return(result)
        }
      )
  )

  # create executionIds
  settingCols <- c(
    "minPriorObservation", "outcomeWashoutDays",
    "casePreTargetDuration", "casePostOutcomeDuration",
    "riskWindowStart", "startAnchor",
    "riskWindowEnd", "endAnchor"
  )
  executionSettings <- unique(caseCombinations[, settingCols])
  executionSettings$settingId <- createExecutionIds(nrow(executionSettings))
  caseCombinations <- merge(caseCombinations, executionSettings, by = settingCols)

  # create thread split
  threadCombinations <- caseCombinations %>%
    dplyr::select(
      "targetId",
      "minPriorObservation",
      "outcomeWashoutDays",
      "casePreTargetDuration",
      "casePostOutcomeDuration"
    ) %>%
    dplyr::distinct()
  threadCombinations$thread <- rep(1:threads, ceiling(nrow(threadCombinations) / threads))[1:nrow(threadCombinations)]
  caseCombinations <- merge(caseCombinations, threadCombinations, by = c(
    "targetId",
    "minPriorObservation",
    "outcomeWashoutDays",
    "casePreTargetDuration",
    "casePostOutcomeDuration"
  ))

  executionCols <- c(
    "minPriorObservation", "outcomeWashoutDays",
    "casePreTargetDuration", "casePostOutcomeDuration"
  )
  executions <- unique(caseCombinations[, executionCols])

  # now create the settings
  for (j in 1:nrow(executions)) {
    settingVal <- executions[j, ]

    restrictedData <- caseCombinations %>%
      dplyr::inner_join(settingVal, by = executionCols)

    for (i in unique(restrictedData$thread)) {
      ind <- restrictedData$thread == i
      settings <- rbind(
        settings,
        data.frame(
          functionName = "computeCaseAggregateCovariateAnalyses",
          settings = as.character(ParallelLogger::convertSettingsToJson(
            list(
              targetIds = unique(restrictedData$targetId[ind]),
              outcomeIds = unique(restrictedData$outcomeId[ind]),
              minPriorObservation = unique(restrictedData$minPriorObservation[ind]),
              outcomeWashoutDays = unique(restrictedData$outcomeWashoutDays[ind]),
              tar = unique(data.frame(
                riskWindowStart = restrictedData$riskWindowStart[ind],
                startAnchor = restrictedData$startAnchor[ind],
                riskWindowEnd = restrictedData$riskWindowEnd[ind],
                endAnchor = restrictedData$endAnchor[ind]
              )),
              casePreTargetDuration = unique(restrictedData$casePreTargetDuration[ind]),
              casePostOutcomeDuration = unique(restrictedData$casePostOutcomeDuration[ind]),
              covariateSettingsJson = combineCovariateSettingsJsons(as.list(restrictedData$covariateSettingsJson[ind])),
              caseCovariateSettingsJson = combineCovariateSettingsJsons(as.list(restrictedData$caseCovariateSettingsJson[ind])),
              settingIds = unique(restrictedData$settingId[ind])
            )
          )),
          executionFolder = paste("cac", i, paste0(settingVal, collapse = "_"), sep = "_"),
          jobId = paste("cac", i, paste0(settingVal, collapse = "_"), sep = "_")
        )
      )
    }
  }

  return(settings)
}
