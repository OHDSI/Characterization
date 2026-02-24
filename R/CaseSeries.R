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
#' @param limitToFirstInNDays whether to limit each target cohort to the first entry into the cohort per N days per subject
#' @param minPriorObservation The minimum time (in days) in the database a patient in the target cohorts must be observed prior to index
#' @param outcomeWashoutDays Patients with the outcome within outcomeWashout days prior to index are excluded from the risk factor analysis
#' @template timeAtRisk
#' @param caseCovariateSettings An object created using \code{createDuringCovariateSettings}
#' @param casePreTargetDuration    The number of days prior to case index we use for FeatureExtraction
#' @param casePostOutcomeDuration    The number of days prior to case index we use for FeatureExtraction
#' @family Aggregate
#' @return
#' A list with the settings
#'
#' @examples
#'
#' caseSeriesSetting <- createCaseSeriesSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = c(3),
#'   limitToFirstInNDays = 365,
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
createCaseSeriesSettings <- function(
    targetIds,
    outcomeIds,
    limitToFirstInNDays = 0,
    minPriorObservation = 0,
    outcomeWashoutDays = 0,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
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
    casePostOutcomeDuration = 365
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

  # check minPriorObservation
  .checkMinPriorObservation(
    minPriorObservation = minPriorObservation,
    errorMessages = errorMessages
  )

  # add check for outcomeWashoutDays and nlimitToFirstInNDays

  # check caseCovariateSettings as only works for During covaraiates
  # check temporal is false
  if (inherits(caseCovariateSettings, "covariateSettings")) {
    caseCovariateSettings <- list(caseCovariateSettings)
  }
  if (sum(unlist(lapply(caseCovariateSettings, function(x) {
    x$temporal
  }))) > 0) {
    stop("Temporal covariateSettings not supported by createCaseSeriesSettings()")
  }

  # check correct caseCovariateSettings
  if(sum(unlist(lapply(
    X = caseCovariateSettings,
    FUN = function(x){
      attr(x,"fun") == "Characterization::getDbDuringCovariateData"
    })), na.rm = TRUE) != length(caseCovariateSettings)){
    stop('caseCoveriateSettings must be Characterization::getDbDuringCovariateData')
  }

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
    caseCovariateSettings = caseCovariateSettings,
    casePreTargetDuration = casePreTargetDuration,
    casePostOutcomeDuration = casePostOutcomeDuration
  )

  class(result) <- "caseSeriesSettings"
  return(result)
}


# TODO cdmVersion should be in runChar
computeCaseSeriesAnalyses <- function(
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
    minCovariateCount = 0,
    minCellCount = 0,
    progressBar = interactive(),
    executionId,
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  message("Case series analysis: connecting to database")

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # 1) create all the t, e and o cohorts using the defined inclusion criteria
  start <- Sys.time()
  message("Case series analysis: Finding temp Ids")

  targetIds <- lookupTargets(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = targetSettingsTable,
    tempEmulationSchema = tempEmulationSchema,
    targetIds = paste0(unique(settings$targetIds), collapse = ','),
    limitToFirstInNDays = settings$limitToFirstInNDays,
    minPriorObservation = settings$minPriorObservation
  )

  caseIds <- lookupCases(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = caseSettingsTable,
    tempEmulationSchema = tempEmulationSchema,
    characterizationTargetIds = paste0(unique(targetIds$characterizationTargetId), collapse = ','),
    outcomeIds = paste0(unique(settings$outcomeIds), collapse = ','),
    outcomeWashoutDays = settings$outcomeWashoutDays,
    startAnchor = settings$startAnchor,
    riskWindowStart = settings$riskWindowStart,
    endAnchor = settings$endAnchor,
    riskWindowEnd = settings$riskWindowEnd
  )

  completionTime <- Sys.time() - start
  message(paste0("Case series analysis: Finding temp Ids took ", round(completionTime, digits = 1), " ", units(completionTime)))


  ## 4) run FE with all the cohorts of interest - ideally inserting the aggregate features into a new table
  start <- Sys.time()
  message("Case series analysis: Running FeatureExtraction")

  FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = characterizationTable,
    cohortDatabaseSchema = characterizationDatabaseSchema,
    cohortIds = c(caseIds$characterizationCaseId*10+3,
                  caseIds$characterizationCaseId*10+4,
                  caseIds$characterizationCaseId*10+5),
    rowIdField = 'row_number',
    covariateSettings = ParallelLogger::convertJsonToSettings(settings$covariateSettings),
    aggregated = TRUE,
    minCharacterizationMean = minCharacterizationMean,

    exportToTable = TRUE,
    targetDatabaseSchema = NULL,
    targetCovariateTable = '#fe_covariate_case',
    targetCovariateContinuousTable = '#fe_covariate_continuous_case',
    targetCovariateRefTable = '#fe_covariate_ref_case',
    targetAnalysisRefTable = '#fe_analysis_ref_case',
    targetTimeRefTable = '#fe_time_ref_case',
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )

  completionTime <- Sys.time() - start
  message(paste0("Case series analysis: Running FeatureExtraction took ", round(completionTime, digits = 1), " ", units(completionTime)))

  # run the case series extraction for binary
  start <- Sys.time()
  message("Case series analysis: Extracting case series covariates")

  result <- Andromeda::andromeda()

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'CaseSeriesBinaryExtraction.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_fe_table = '#fe_covariate_case',
    cohort_definition_ids = paste0(c(caseIds$characterizationCaseId*10+3,
                                     caseIds$characterizationCaseId*10+4,
                                     caseIds$characterizationCaseId*10+5),
                                   collapse = ','),
    min_count = minCovariateCount
  )

  tryCatch(
    {DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = result,
      andromedaTableName = 'caseSeriesCovariates',
      snakeCaseToCamelCase = TRUE
    )},
    error = function(e){message(e); return(NULL)}
  )

  # continuous code here
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'CaseSeriesContinuousExtraction.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_fe_table = '#fe_covariate_continuous_case',
    cohort_definition_ids = paste0(c(caseIds$characterizationCaseId*10+3,
                                     caseIds$characterizationCaseId*10+4,
                                     caseIds$characterizationCaseId*10+5),
                                   collapse = ','),
    min_count = minCovariateCount
  )

  tryCatch(
    {DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = result,
      andromedaTableName = 'caseSeriesCovariatesContinuous',
      snakeCaseToCamelCase = TRUE
    )},
    error = function(e){message(e)}
  )

  # extract the covariate_ref and analysis_ref
  message("Case series analysis: Downloading ref tables")

  # extract the covariate ref and analysis_ref tables as well
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = paste0("SELECT * from #fe_covariate_ref_case;"),
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
      sql = paste0("SELECT * from #fe_analysis_ref_case;"),
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
    ),
    andromeda = result,
    andromedaTableName = 'analysisRef',
    snakeCaseToCamelCase = TRUE
  )

  result$targetSettings <- targetIds
  result$caseSettings <- caseIds

  completionTime <- Sys.time() - start
  message(paste0("Case series analysis: Downloading took ", round(completionTime, digits = 1), " ", units(completionTime)))

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
    sqlFilename = 'DropCaseSeriesTempTables.sql',
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

getCaseSeriesJobs <- function(
    characterizationSettings,
    nTargetJobs
    ) {

  characterizationSettings <- characterizationSettings$caseSeriesSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }

  # get all the settings
  # targetId, minPriorObservation, outcomeId, outcomeWashoutDays, tar, covariateSettings

  # split the targetIds per setting into nTargetJobs groups

  caseSeriesCombinations <- do.call(
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
                  targetId = unique(characterizationSettings[[i]]$targetIds),
                  limitToFirstInNDays = characterizationSettings[[i]]$limitToFirstInNDays,
                  minPriorObservation = characterizationSettings[[i]]$minPriorObservation,

                  outcomeId = outcomeId,
                  outcomeWashoutDays = unique(characterizationSettings[[i]]$outcomeWashoutDays),
                  riskWindowStart = unique(characterizationSettings[[i]]$riskWindowStart),
                  startAnchor = unique(characterizationSettings[[i]]$startAnchor),
                  riskWindowEnd = unique(characterizationSettings[[i]]$riskWindowEnd),
                  endAnchor = unique(characterizationSettings[[i]]$endAnchor),

                  casePreTargetDuration = unique(characterizationSettings[[i]]$casePreTargetDuration),
                  casePostOutcomeDuration = unique(characterizationSettings[[i]]$casePostOutcomeDuration),

                  covariateSettingsJson = as.character(ParallelLogger::convertSettingsToJson(characterizationSettings[[i]]$caseCovariateSettings))
                )
              }
            )
          )
        }
      )
  )

  settings <- c()
  if(nrow(caseSeriesCombinations) > 0 ){
    jobCols <- c("targetId")

    settingCols <- c(
      "limitToFirstInNDays", "minPriorObservation",
      "outcomeWashoutDays",
      "riskWindowStart", "startAnchor",
      "riskWindowEnd", "endAnchor",
      "casePreTargetDuration", "casePostOutcomeDuration"
    )

    jobSettings <- caseSeriesCombinations %>%
      dplyr::select(dplyr::all_of(jobCols)) %>%
      dplyr::distinct()
    jobSettings$nTargetJobs <- rep(1:nTargetJobs, ceiling(nrow(jobSettings) / nTargetJobs))[1:nrow(jobSettings)]
    caseSeriesCombinations <- merge(caseSeriesCombinations, jobSettings, by = jobCols)

    executionSettings <- caseSeriesCombinations %>%
      dplyr::select(dplyr::all_of(settingCols)) %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        settingId = dplyr::row_number()
      )

    caseSeriesCombinations <- merge(caseSeriesCombinations, executionSettings, by = settingCols)

    for (settingId in unique(caseSeriesCombinations$settingId)) {

      restrictedData <- caseSeriesCombinations %>%
        dplyr::filter(.data$settingId == !!settingId)

      settingVal <- restrictedData[1,settingCols]

      for (i in unique(restrictedData$nTargetJobs)) {
        ind <- restrictedData$nTargetJobs== i

        settings <- rbind(
          settings,
          data.frame(
            functionName = "computeCaseSeriesAnalyses",
            settings = as.character(ParallelLogger::convertSettingsToJson(
              list(
                targetIds = unique(restrictedData$targetId[ind]),
                outcomeIds = unique(restrictedData$outcomeId[ind]),
                minPriorObservation = unique(restrictedData$minPriorObservation[ind]),
                limitToFirstInNDays = unique(restrictedData$limitToFirstInNDays[ind]),

                outcomeWashoutDays = unique(restrictedData$outcomeWashoutDays[ind]),
                riskWindowStart = unique(restrictedData$riskWindowStart[ind]),
                startAnchor = unique(restrictedData$startAnchor[ind]),
                riskWindowEnd = unique(restrictedData$riskWindowEnd[ind]),
                endAnchor = unique(restrictedData$endAnchor[ind]),

                casePreTargetDuration = unique(restrictedData$casePreTargetDuration[ind]),
                casePostOutcomeDuration = unique(restrictedData$casePostOutcomeDuration[ind]),

                covariateSettingsJson = combineCovariateSettingsJsons(as.list(unique(restrictedData$covariateSettingsJson[ind])))
              )
            )),
            executionFolder = paste("cs",i, paste0(settingVal, collapse = "_"), sep = "_"),
            jobId = paste("cs",i, paste0(settingVal, collapse = "_"), sep = "_")
          )
        )
      }
    }
  }

  # takes all the settings and break them down into
  # a list of lists with the function to execute and inputs
  return(settings)
}


