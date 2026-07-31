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
#' @param studyPopulationSettings A list of objects created using \code{createStudyPopulationSettings} that specifies target cohorts and inclusion criteria
#' @param outcomeIds  A list of cohortIds for the outcome cohorts
#' @param outcomeWashoutDays A single integer value. Patients with the outcome within outcomeWashout days prior to index are excluded from the risk factor analysis
#' @template timeAtRisk
#' @param covariateSettings   An object created using \code{FeatureExtraction::createCovariateSettings}
#'
#' @family Aggregate
#' @return
#' A list with the settings
#'
#' @examples
#'
#' riskFactorSetting <- createRiskFactorSettings(
#'   studyPopulationSettings = createStudyPopulationSettings(
#'     targetIds = c(1,2),
#'     minPriorObservation = 365,
#'     limitToFirstInNDays = 99999
#'   ),
#'   outcomeIds = c(3),
#'   outcomeWashoutDays = 90,
#'   riskWindowStart = 1,
#'   startAnchor = "cohort start",
#'   riskWindowEnd = 365,
#'   endAnchor = "cohort start"
#' )
#'
#' @export
createRiskFactorSettings <- function(
    studyPopulationSettings,
    outcomeIds,
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
    )
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

  # check outcomeWashoutDays is length 1
  if (length(outcomeWashoutDays) > 1) {
    stop("Please add one outcomeWashoutDays per setting")
  }

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
  #.checkMinPriorObservation(
  #  minPriorObservation = minPriorObservation,
  #  errorMessages = errorMessages
  #)

  # add check for outcomeWashoutDays

  checkmate::reportAssertions(errorMessages)

  # check unique Ts and Os
  #if (length(targetIds) != length(unique(targetIds))) {
  #  message("targetIds have duplicates - making unique")
  #  targetIds <- unique(targetIds)
  #}
  if (length(outcomeIds) != length(unique(outcomeIds))) {
    message("outcomeIds have duplicates - making unique")
    outcomeIds <- unique(outcomeIds)
  }


  # create list
  result <- list(
    studyPopulationSettings = combineStudyPopulationSettings(studyPopulationSettings),
    outcomeIds = outcomeIds,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = gsub(' ', '_',startAnchor),
    riskWindowEnd = riskWindowEnd,
    endAnchor = gsub(' ', '_',endAnchor),
    covariateSettings = covariateSettings # risk factors
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
    caseSettingsTable, # contains map between settings and case id
    caseCountTable, # new

    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder,
    minCharacterizationMean = 0,
    minSMD = 0.1,
    minCovariateCount = 0,
    minCellCount = 0,
    progressBar = interactive(),
    mode,
    executionId,
    minCaseSize, #new
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  message("Risk factor analysis: connecting to database")

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )


  # 1) create all the t, e and o cohorts using the defined inclusion criteria
  start <- Sys.time()
  message("Risk factor analysis: Finding temp Ids")

  # TODO update this using settings$studyPopulationSettings

  caseIds <- lookupCases(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = caseSettingsTable,
    countTable = caseCountTable,
    tempEmulationSchema = tempEmulationSchema,
    characterizationTargetIds = paste0(unique(settings$characterizationTargetId), collapse = ','),
    outcomeIds = paste0(unique(settings$outcomeIds), collapse = ','),
    outcomeWashoutDays = settings$outcomeWashoutDays,
    startAnchor = settings$startAnchor,
    riskWindowStart = settings$riskWindowStart,
    endAnchor = settings$endAnchor,
    riskWindowEnd = settings$riskWindowEnd,
    minCaseSize = minCaseSize,
    applyMinSizeToNonCases = TRUE
  )

  # generate the targets, cases and non-cases ids
  # what about target only in efficient mode caseIds$characterizationTargetId*10
  if(mode == 'Efficient'){
    message('Efficient mode so will not generate features on non-cases')
    cohortIds <- c(caseIds$characterizationCaseId*10+1,caseIds$characterizationTargetId)
  } else{
    message(paste0(mode,' mode so will generate features on non-cases'))
    cohortIds <- c(caseIds$characterizationCaseId*10+1,caseIds$characterizationCaseId*10+2)
  }

  completionTime <- Sys.time() - start
  message(paste0("Risk factor analysis: Finding temp Ids took ", round(completionTime, digits = 1), " ", units(completionTime)))

  if(length(cohortIds) == 0){
    message('No cohorts with number of people >= minSize')
    return(invisible(TRUE))
  }

  ## 2) run FE with all the cohorts of interest - ideally inserting the aggregate features into a new table
  start <- Sys.time()
  message("Risk factor analysis: Running FeatureExtraction")
  FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = characterizationTable,
    cohortDatabaseSchema = characterizationDatabaseSchema,
    cohortIds = cohortIds,
    rowIdField = 'row_id',
    covariateSettings = ParallelLogger::convertJsonToSettings(settings$covariateSettings),
    aggregated = TRUE,
    minCharacterizationMean = 0, #minCharacterizationMean,

    exportToTable = TRUE,
    targetDatabaseSchema = NULL,
    targetCovariateTable = '#fe_covariate_rf',
    targetCovariateContinuousTable = '#fe_covariate_continuous_rf',
    targetCovariateRefTable = '#fe_covariate_ref_rf',
    targetAnalysisRefTable = '#fe_analysis_ref_rf',
    targetTimeRefTable = '#fe_time_ref_rf',
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )

  completionTime <- Sys.time() - start
  message(paste0("Risk factor analysis: Running FeatureExtraction took ", round(completionTime, digits = 1), " ", units(completionTime)))



  ## 3) for each target,exclude,cases join the tables and calculate the SMD
  start <- Sys.time()
  message("Risk factor analysis: Calculating SMD for binary")

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'RiskFactorBinaryExtraction.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,

    case_settings_table = caseSettingsTable,
    characterization_case_ids = paste0(caseIds$characterizationCaseId, collapse = ','),
    characterization_fe_table = '#fe_covariate_rf',
    efficient_mode =  mode == 'Efficient',
    smd_min = minSMD,
    min_count = minCovariateCount,
    min_characterization_mean = minCharacterizationMean
  )

  result <- Andromeda::andromeda()

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = result,
    andromedaTableName = 'riskFactorCovariates',
    snakeCaseToCamelCase = TRUE
    )

  message("Risk factor analysis: Calculating SMD for continuous")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'RiskFactorContinuousExtraction.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,

    case_settings_table = caseSettingsTable,
    characterization_case_ids = paste0(caseIds$characterizationCaseId, collapse = ','),
    characterization_fe_table = '#fe_covariate_continuous_rf',
    efficient_mode = mode == 'Efficient',
    smd_min = minSMD,
    min_count = minCovariateCount
  )

  tryCatch({DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = sql,
    andromeda = result,
    andromedaTableName = 'riskFactorCovariatesContinuous',
    snakeCaseToCamelCase = TRUE
  )}, error = function(e){
    message(e);
  })

  # cohort_id, cohort_setting_id (hash), outcome_id, outcome_setting_id, covariate_id, non_case_count, non_case_mean, case_count, case_mean, smd

  message("Risk factor analysis: Downloading ref tables")

  # extract the covariate ref and analysis_ref tables as well
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = paste0("SELECT * from #fe_covariate_ref_rf;"),
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
      ),
    andromeda = result,
    andromedaTableName = 'covariateRef',
    snakeCaseToCamelCase = TRUE
  )

  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = paste0("SELECT * from #fe_analysis_ref_rf;"),
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
    ),
    andromeda = result,
    andromedaTableName = 'analysisRef',
    snakeCaseToCamelCase = TRUE
  )

  # TODO - what is this used for?
  ##result$targetSettings <- settings$characterizationTargetId
  ##result$caseSettings <- caseIds

  completionTime <- Sys.time() - start
  message(paste0("Risk factor analysis: Calculating SMD and downloading took ", round(completionTime, digits = 1), " ", units(completionTime)))

  # export to andromeda
  result <- addDbAndSettings(
    andromeda = result,
    databaseId = databaseId,
    settingId = executionId
  )
  saveCharacterizationAndromeda(
    andromeda = result,
    outputFolder = outputFolder
  )

  # clean up temp tables (as some dbms do not have temp tables and it can get messy)
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'DropRiskFactorTempTables.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql
  )

  return(invisible(TRUE))
}



# function to partition jobs
getRiskFactorJobs <- function(
    characterizationSettings,
    nTargetJobs # currently not used
    ) {

  characterizationSettings <- characterizationSettings$riskFactorSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }

  # get all the settings
  # targetId, minPriorObservation, outcomeId, outcomeWashoutDays, tar, covariateSettings

  riskFactorCombinations <- do.call(
    what = "rbind",
    args =
      lapply(
        X = 1:length(characterizationSettings),
        FUN = function(i) {
          do.call(
            what = "rbind",
            args = lapply(
              X = unique(characterizationSettings[[i]]$outcomeIds),
              FUN = function(outcomeId){

                data.frame(
                  characterizationTargetId = unique(characterizationSettings[[i]]$characterizationTargetIds),

                  outcomeId = outcomeId,
                  outcomeWashoutDays = unique(characterizationSettings[[i]]$outcomeWashoutDays),
                  riskWindowStart = unique(characterizationSettings[[i]]$riskWindowStart),
                  startAnchor = unique(characterizationSettings[[i]]$startAnchor),
                  riskWindowEnd = unique(characterizationSettings[[i]]$riskWindowEnd),
                  endAnchor = unique(characterizationSettings[[i]]$endAnchor),

                  covariateSettingsJson = as.character(ParallelLogger::convertSettingsToJson(characterizationSettings[[i]]$covariateSettings))
                )
              }
            )
          )
        }
      )
  )

  settings <- c()
  if(nrow(riskFactorCombinations) > 0 ){
    jobCols <- c("characterizationTargetId")
    settingCols <- c(
      "outcomeWashoutDays",
      "riskWindowStart", "startAnchor",
      "riskWindowEnd", "endAnchor"
    )

    jobSettings <- riskFactorCombinations %>%
      dplyr::select(dplyr::all_of(jobCols)) %>%
      dplyr::distinct()
    jobSettings$nTargetJobs <- rep(1:nTargetJobs, ceiling(nrow(jobSettings) / nTargetJobs))[1:nrow(jobSettings)]
    riskFactorCombinations <- merge(riskFactorCombinations, jobSettings, by = jobCols)


    executionSettings <- riskFactorCombinations %>%
      dplyr::select(dplyr::all_of(settingCols)) %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        settingId = dplyr::row_number()
      )

    riskFactorCombinations <- merge(riskFactorCombinations, executionSettings, by = settingCols)


    # split by settingId (in future add split by target as well?)

    # now create the settings
    for (settingId in unique(riskFactorCombinations$settingId)) {

      restrictedData <- riskFactorCombinations %>%
        dplyr::filter(.data$settingId == !!settingId)

      settingVal <- restrictedData[1,settingCols]

      for (i in unique(restrictedData$nTargetJobs)) {
        ind <- restrictedData$nTargetJobs== i

        settings <- rbind(
          settings,
          data.frame(
            functionName = "computeRiskFactorAnalyses",
            settings = as.character(ParallelLogger::convertSettingsToJson(
              list(
                characterizationTargetIds = unique(restrictedData$characterizationTargetId[ind]),
                outcomeIds = unique(restrictedData$outcomeId[ind]),
                outcomeWashoutDays = unique(restrictedData$outcomeWashoutDays[ind]),
                riskWindowStart = unique(restrictedData$riskWindowStart[ind]),
                startAnchor = unique(restrictedData$startAnchor[ind]),
                riskWindowEnd = unique(restrictedData$riskWindowEnd[ind]),
                endAnchor = unique(restrictedData$endAnchor[ind]),

                covariateSettingsJson = combineCovariateSettingsJsons(as.list(unique(restrictedData$covariateSettingsJson[ind])))
                #settingIds = unique(restrictedData$settingId[ind])
              )
            )),
            executionFolder = paste("rf",i, paste0(settingVal, collapse = "_"), sep = "_"),
            jobId = paste("rf",i, paste0(settingVal, collapse = "_"), sep = "_")
          )
        )
      }
    }
  }

  # takes all the settings and break them down into
  # a list of lists with the function to execute and inputs
  return(settings)
}





