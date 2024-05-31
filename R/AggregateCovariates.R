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
    casePostOutcomeDuration = casePostOutcomeDuration
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
#' @param outputFolder The location to save the results as csv files
#' @param runId  (depreciated) Unique identifier for the covariate setting
#' @param threads The number of threads to run in parallel
#' @param incrementalFile (optional) A file that tracks completed studies
#' @param minCharacterizationMean The minimum mean value for characterization output. Values below this will be cut off from output. This
#'                                will help reduce the file size of the characterization output, but will remove information
#'                                on covariates that have very low values. The default is 0.
#' @param runExtractTfeatures        Whether to extract the target cohort features
#' @param runExtractOfeatures        Whether to extract the outcome cohort features
#' @param runExtractCaseFeatures    Whether to extract the case cohort features
#' @param settingIds     (not recommended to use) User can specify the lookup ids for the settings
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
    outputFolder = file.path(getwd(),'characterization_results'),
    runId = 1, # not needed
    threads = 1,
    incrementalFile = NULL,
    minCharacterizationMean = 0,
    runExtractTfeatures = T,
    runExtractOfeatures = T,
    runExtractCaseFeatures = T,
    settingIds = NULL
    ) {
  # check inputs

  start <- Sys.time()

  # if not a list of aggregateCovariateSettings make it one
  if(!inherits(aggregateCovariateSettings, 'list')){
    aggregateCovariateSettings <- list(aggregateCovariateSettings)
  }

  # check that covariateSettings are identical across aggregateCovariateSettings
  if(length(aggregateCovariateSettings)>1){
    if(sum(unlist(lapply(2:length(aggregateCovariateSettings),
                         function(i){
                           identical(
                             aggregateCovariateSettings[[1]]$covariateSettings,
                             aggregateCovariateSettings[[i]]$covariateSettings
                           )
                         }))) != (length(aggregateCovariateSettings)-1)){
      stop('Cannot have different covariate settings')
    }
  }

  # create covariateSetting lookups
  #=================================
  covariateSettingsList <- extractCovariateList(aggregateCovariateSettings)
  caseCovariateSettingsList <- extractCaseCovariateList(aggregateCovariateSettings)

  #=================================

  # extract the combinations of
  # T,O,type, tar, washout, min prior obs, case prior, case post
  # case settings, covariate settings, cov hash, case cov hash
  cohortDetails <- do.call(
    what = 'rbind',
    args = lapply(
    X = aggregateCovariateSettings,
    FUN = extractCombinationSettings,
    caseCovariateSettingsList = caseCovariateSettingsList,
    covariateSettingsList = covariateSettingsList
      )
    )

  #remove redundancy
  cohortDetails <- unique(cohortDetails)

  # get settings
  settingColumns <- c(
    'minPriorObservation', 'outcomeWashoutDays',
    'casePreTargetDuration', 'casePostOutcomeDuration',
    'riskWindowStart','startAnchor',
    'riskWindowEnd','endAnchor',
    'covariateSettingsHash', 'caseCovariateSettingsHash'
  )
  settings <- unique(cohortDetails[,settingColumns])
  if(is.null(settingIds)){
    settings$settingId <- 1:nrow(settings)
  } else{
    if(nrow(settings) == settingIds){
    settings$settingId <- settingIds
    } else{
      stop('SettingsIds input but wrong length')
    }
  }
  # add settingId to cohortDetails
  cohortDetails <- merge(
    x = cohortDetails,
    y = settings,
    by = settingColumns
  )

  # save settings, cohort_details
  saveSettings(
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    databaseId = databaseId,
    covariateSettingsList = covariateSettingsList,
    caseCovariateSettingsList = caseCovariateSettingsList
    )


  # add cohortDefinitionId
  cohortDetails$cohortDefinitionId <- 1:nrow(cohortDetails)

  # add folder Id before incremental so folders the same
  # each time
  cohortDetails <- addFolderId(
    cohortDetails = cohortDetails,
    outputFolder = file.path(outputFolder,'execution'),
    threads = threads
  )

  # if running incremental remove previously executed
  # analyses
  if(!is.null(incrementalFile)){
    if(file.exists(incrementalFile)){
      executedDetails <- loadIncremental(incrementalFile)
      cohortDetails <- removeExecuted(
        cohortDetails = cohortDetails,
        executedDetails = executedDetails
      )
    }
  }

  # stop if results already exist
  if(nrow(cohortDetails) == 0){
    message('All results have been previosuly executed')
  } else{

    # RUN ANALYSES
    message('Creating new cluster')
    cluster <- ParallelLogger::makeCluster(
      numberOfThreads = threads,
      singleThreadToMain = T,
      setAndromedaTempFolder = T
    )

    # 1) get the T and folders to get jobs run in parallel
    if(runExtractTfeatures){
      start <- Sys.time()
      ind <- cohortDetails$cohortType %in% c('Target','Tall')
      if( sum(ind) > 0 ){
        message('Running target cohort features')
        runs <- unique(cohortDetails$folderId[ind])
        inputList <- lapply(
          X = runs,
          FUN = function(folderId){
            list(
              connectionDetails = connectionDetails,
              cdmDatabaseSchema = cdmDatabaseSchema,
              cohortDetails = cohortDetails[cohortDetails$folderId == folderId,],
              covariateSettingsList = covariateSettingsList,
              targetDatabaseSchema = targetDatabaseSchema,
              targetTable = targetTable,
              tempEmulationSchema = tempEmulationSchema,
              minCharacterizationMean = minCharacterizationMean,
              cdmVersion = cdmVersion,
              databaseId = databaseId,
              incrementalFile = incrementalFile
            )
          }
        )

        ParallelLogger::clusterApply(
          cluster = cluster,
          x = inputList,
          fun = extractTargetFeatures
        )
        end <- Sys.time() - start
        message(
          paste0(
            'Extracting target cohort features took ',
            round(end, digits = 2), ' ',
            units(end)
          )
        )
      }
    }

    # 2) get the outcomes and folders to get jobs run in parallel
    if(runExtractOfeatures){
      start <- Sys.time()
      ind <- cohortDetails$cohortType %in% c('Outcome','Oall')
      if( sum(ind) > 0 ){
        message('Running outcome cohort features')
        runs <- unique(cohortDetails$folderId[ind])
        inputList <- lapply(
          X = runs,
          FUN = function(folderId){
            list(
              connectionDetails = connectionDetails,
              cdmDatabaseSchema = cdmDatabaseSchema,
              cohortDetails = cohortDetails[cohortDetails$folderId == folderId,],
              covariateSettingsList = covariateSettingsList,
              outcomeDatabaseSchema = outcomeDatabaseSchema,
              outcomeTable = outcomeTable,
              tempEmulationSchema = tempEmulationSchema,
              minCharacterizationMean = minCharacterizationMean,
              cdmVersion = cdmVersion,
              databaseId = databaseId,
              incrementalFile = incrementalFile
            )
          }
        )

        ParallelLogger::clusterApply(
          cluster = cluster,
          x = inputList,
          fun = extractOutcomeFeatures
        )
        end <- Sys.time() - start
        message(
          paste0(
            'Extracting outcome cohort features took ',
            round(end, digits = 2), ' ',
            units(end)
          )
        )
      }
    }


    # 3) get the cases and folders to get jobs run in parallel
    if(runExtractCaseFeatures){
      start <- Sys.time()
      ind <- !cohortDetails$cohortType %in% c('Outcome','Oall', 'Target', 'Tall')
      if( sum(ind) > 0 ){
        message('Running case cohort features')
        runs <- unique(cohortDetails$folderId[ind])
        inputList <- lapply(
          X = runs,
          FUN = function(folderId){
            list(
              connectionDetails = connectionDetails,
              cdmDatabaseSchema = cdmDatabaseSchema,
              cohortDetails = cohortDetails[cohortDetails$folderId == folderId,],
              covariateSettingsList = covariateSettingsList,
              caseCovariateSettingsList = caseCovariateSettingsList,
              targetDatabaseSchema = targetDatabaseSchema,
              targetTable = targetTable,
              outcomeDatabaseSchema = outcomeDatabaseSchema,
              outcomeTable = outcomeTable,
              tempEmulationSchema = tempEmulationSchema,
              minCharacterizationMean = minCharacterizationMean,
              cdmVersion = cdmVersion,
              databaseId = databaseId,
              incrementalFile = incrementalFile
            )
          }
        )

        ParallelLogger::clusterApply(
          cluster = cluster,
          x = inputList,
          fun = extractCaseFeatures
        )
        end <- Sys.time() - start
        message(
          paste0(
            'Extracting case cohort features took ',
            round(end, digits = 2), ' ',
            units(end)
          )
        )
      }
    }

    # finish cluster
    message('Stopping cluster')
    ParallelLogger::stopCluster(cluster = cluster)
  }
  # combine csvs into one
  message('Aggregating csv files')
  aggregateCsvs(
    outputFolder = outputFolder
  )

  return(invisible(TRUE))
}

