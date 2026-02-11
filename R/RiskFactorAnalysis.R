# Copyright 2026 Observational Health Data Sciences and Informatics
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

#' Create risk factor study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param outcomeIds  A list of cohortIds for the outcome cohorts
#' @param limitToFirstInNDays whether to limit each target cohort to the first entry into the cohort per N days per subject
#' @param minPriorObservation The minimum time (in days) in the database a patient in the target cohorts must be observed prior to index
#' @param outcomeWashoutDays Patients with the outcome within outcomeWashout days prior to index are excluded from the risk factor analysis
#' @template timeAtRisk
#' @param covariateSettings   An object created using \code{FeatureExtraction::createCovariateSettings}
#' @param minTargetSize The minimum size of the target cohorts for them to have aggregate covariates calculated
#' @param minTwithOSize The minimum size of the cohorts corresponding to patients in the target with the outcome during time-at-risk for them to have aggregate covariates calculated
#'
#' @family Aggregate
#' @return
#' A list with the settings
#'
#' @examples
#'
#' riskFactorSetting <- createRiskFactorSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = c(3),
#'   minPriorObservation = 365,
#'   outcomeWashoutDays = 90,
#'   riskWindowStart = 1,
#'   startAnchor = "cohort start",
#'   riskWindowEnd = 365,
#'   endAnchor = "cohort start"
#' )
#'
#' @export
createRiskFactorSettings <- function(
    targetIds,
    outcomeIds,
    # targetInclusionSettings - limitToFirstExposure, minPriorObservation
    # outcomeInclusionSettings - outcomeWashoutDays
    #? indicationIds
    limitToFirstInNDays = 0,
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
    minTargetSize = 0,
    minTwithOSize = 0
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
    limitToFirstInNDays = limitToFirstInNDays,
    minPriorObservation = minPriorObservation,
    outcomeIds = outcomeIds,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = gsub(' ', '_',startAnchor),
    riskWindowEnd = riskWindowEnd,
    endAnchor = gsub(' ', '_',endAnchor),
    covariateSettings = covariateSettings, # risk factors
    minTargetSize = minTargetSize,
    minTwithOSize = minTwithOSize
  )

  class(result) <- "riskFactorSettings"
  return(result)
}

createExecutionIds <- function(size) {
  executionIds <- gsub(" ", "", gsub("[[:punct:]]", "", paste(Sys.time(), sample(1000000, size), sep = "")))
  return(executionIds)
}

# TODO cdmVersion should be in runChar
computeRiskFactorAnalyses <- function(
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cdmVersion = 5,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema, # remove
    outcomeTable = targetTable, # remove

    characterizationDatabaseSchema,
    characterizationTable, # contains char cohorts
    targetSettingsTable, # contains map between settings and char cohort id
    caseSettingsTable, # contains map between settings and case id

    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder,
    minCharacterizationMean = 0,
    minSMD = 0.1,
    minCellCount = 0,
    progressBar = interactive(),
    mode,
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  message("Target Aggregate:  starting")

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )


  # 1) create all the t, e and o cohorts using the defined inclusion criteria
  start <- Sys.time()
  message("Risk factor analysis: Finding temp Ids")

  caseIds <- lookupCases(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = caseSettingsTable,
    tempEmulationSchema = tempEmulationSchema,
    characterizationTargetIds = settings$characterizationTargetIds,
    outcomeIds = settings$outcomeIds,
    outcomeWashoutDays = settings$outcomeWashoutDays,
    startAnchor = settings$startAnchor,
    riskWindowStart = settings$riskWindowStart,
    endAnchor = settings$endAnchor,
    riskWindowEnd = settings$riskWindowEnd
  )

  targetIds <- lookupTargets(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = caseSettingsTable,
    tempEmulationSchema = tempEmulationSchema,
    characterizationTargetIds = settings$characterizationTargetIds
  )

  # generate the targets, cases and non-cases ids
  # what about target only in efficient mode caseIds$characterizationTargetId*10
  cohortIds <- c(settings$characterizationTargetIds*10, (caseIds$characterizationCaseId*10+1), (caseIds$characterizationCaseId*10+2))

  completionTime <- Sys.time() - start
  message(paste0("Risk factor analysis: Finding temp Ids took ", round(completionTime, digits = 1), " ", units(completionTime)))



  ## 2) get attrition
  #start <- Sys.time()
  #message("Risk factor analysis: Extracting cohort attritions")
  # TODO get attrition from CohortGenerator when it is in there

  #completionTime <- Sys.time() - start
  #message(paste0("Risk factor analysis: Extracting cohort attritions took ", round(completionTime, digits = 1), " ", units(completionTime)))


  ## 3) remove small cohorts using attrition
  #start <- Sys.time()
  #message("Risk factor analysis: Discovering small cohorts to ignore")

  #completionTime <- Sys.time() - start
  #message(paste0("Risk factor analysis: Discovering small cohorts to ignore ", round(completionTime, digits = 1), " ", units(completionTime)))


  ## 4) run FE with all the cohorts of interest - ideally inserting the aggregate features into a new table
  start <- Sys.time()
  message("Risk factor analysis: Running FeatureExtraction")
  FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = characterizationTable,
    cohortDatabaseSchema = characterizationDatabaseSchema,
    cohortIds = cohortIds,
    rowIdField = 'row_number',
    covariateSettings = ParallelLogger::convertJsonToSettings(settings$covariateSettings),
    aggregated = TRUE,
    minCharacterizationMean = minCharacterizationMean,

    targetDatabaseSchema = NULL,#characterizationDatabaseSchema,
    targetCovariateTable = '#fe_covariate',
    targetCovariateRefTable = '#fe_covariate_ref',
    targetAnalysisRefTable = '#fe_analysis_ref',
    dropTableIfExists = TRUE,
    createTable = TRUE
  )

  completionTime <- Sys.time() - start
  message(paste0("Risk factor analysis: Running FeatureExtraction took ", round(completionTime, digits = 1), " ", units(completionTime)))



  ## 5) for each target,exclude,cases join the tables and calculate the SMD
  start <- Sys.time()
  message("Risk factor analysis: Calculating SMD for binary")

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'RiskFactorBinaryExtraction.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = Sys.getenv("DATABRICKS_SCRATCH_SCHEMA"),
    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,

    cohort_definition_ids = cohortIds,
    characterization_fe_table = '#fe_covariate',
    efficient_mode =  mode == 'Efficient',
    smd_min = minSMD
  )

  result <- Andromeda::andromeda()

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = result,
    andromedaTableName = 'risk_factor_binary',
    snakeCaseToCamelCase = TRUE,
    appendToTable = TRUE
    )

  message("Risk factor analysis: Calculating SMD for continuous")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'RiskFactorContinuousExtraction.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,

    cohort_definition_ids = cohortIds,
    characterization_fe_table = '#fe_covariate_continuous',
    efficient_mode = mode == 'Efficient',
    smd_min = minSMD
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = result,
    andromedaTableName = 'risk_factor_continuous',
    snakeCaseToCamelCase = TRUE,
    appendToTable = TRUE
  )

  # cohort_id, cohort_setting_id (hash), outcome_id, outcome_setting_id, covariate_id, non_case_count, non_case_mean, case_count, case_mean, smd

  message("Risk factor analysis: Downloading ref tables")

  # extract the covariate ref and analysis_ref tables as well
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = "SELECT * from #fe_covariate_ref;",
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
      ),
    andromeda = result,
    andromedaTableName = 'covariate_ref',
    snakeCaseToCamelCase = TRUE,
    appendToTable = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = "SELECT * from #fe_analysis_ref;",
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
    ),
    andromeda = result,
    andromedaTableName = 'analysis_ref',
    snakeCaseToCamelCase = TRUE,
    appendToTable = TRUE
  )

  result$target_setting <- targetIds
  result$case_setting <- caseIds

  completionTime <- Sys.time() - start
  message(paste0("Risk factor analysis: Calculating SMD and downloading took ", round(completionTime, digits = 1), " ", units(completionTime)))

  return(invisible(result))
}



# function to partition jobs
getRiskFactorJobs <- function(
    characterizationSettings,
    threads) {

  characterizationSettings <- characterizationSettings$riskFactorSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }
  ind <- 1:length(characterizationSettings)

  # get all the settings
  # targetId, minPriorObservation, outcomeId, outcomeWashoutDays, tar, covariateSettings

  riskFactorCombinations <- ''

  # create executionIds
  settingCols <- c(
    "minPriorObservation", "outcomeWashoutDays",
    "riskWindowStart", "startAnchor",
    "riskWindowEnd", "endAnchor"
  )
  executionSettings <- unique(riskFactorCombinations[, settingCols])
  executionSettings$settingId <- createExecutionIds(nrow(executionSettings))
  riskFactorCombinations <- merge(riskFactorCombinations, executionSettings, by = settingCols)

  # create thread split
  threadCombinations <- riskFactorCombinations %>%
    dplyr::select(
      "targetId",
      "minPriorObservation",
      "outcomeWashoutDays"
    ) %>%
    dplyr::distinct()
  threadCombinations$thread <- rep(1:threads, ceiling(nrow(threadCombinations) / threads))[1:nrow(threadCombinations)]
  riskFactorCombinations <- merge(riskFactorCombinations, threadCombinations, by = c(
    "targetId",
    "minPriorObservation",
    "outcomeWashoutDays"
  ))

  executionCols <- c(
    "minPriorObservation", "outcomeWashoutDays"
  )
  executions <- unique(riskFactorCombinations[, executionCols])

  # now create the settings
  for (j in 1:nrow(executions)) {
    settingVal <- executions[j, ]

    restrictedData <- riskFactorCombinations %>%
      dplyr::inner_join(settingVal, by = executionCols)

    for (i in unique(restrictedData$thread)) {
      ind <- restrictedData$thread == i
      settings <- rbind(
        settings,
        data.frame(
          functionName = "computeRiskFactorAnalyses",
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
              covariateSettingsJson = combineCovariateSettingsJsons(as.list(restrictedData$covariateSettingsJson[ind])),
              settingIds = unique(restrictedData$settingId[ind]),
              minTwithOSize = minTwithOSize
            )
          )),
          executionFolder = paste("rf", i, paste0(settingVal, collapse = "_"), sep = "_"),
          jobId = paste("rf", i, paste0(settingVal, collapse = "_"), sep = "_")
        )
      )
    }
  }

  # takes all the settings and break them down into
  # a list of lists with the function to execute and inputs
  return(settings)
}





