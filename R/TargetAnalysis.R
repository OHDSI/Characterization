#' Create target baseline aggregate covariate study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param limitToFirstInNDays Whether to remove target cohort entries that occur within limitToFirstInNDays of a prior entry. limitToFirstInNDays = 99999 means limit to first entry.
#' @param minPriorObservation The minimum time (in days) in the database a patient in the target cohorts must be observed prior to index
#' @param covariateSettings   An object created using \code{FeatureExtraction::createCovariateSettings}
#' @family Aggregate
#' @return
#' A list with the settings
#'
#' @examples
#'
#' aggregateSetting <- createTargetBaselineSettings(
#'   targetIds = c(1,2),
#'   limitToFirstInNDays = 99999,
#'   minPriorObservation = 365
#' )
#'
#' @export
createTargetBaselineSettings <- function(
    targetIds,
    limitToFirstInNDays = 99999,
    minPriorObservation = 0,
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
      useCharlsonIndex = TRUE,
      endDays = 0,
      longTermStartDays = -365,
      shortTermStartDays = -30
    )
) {

  errorMessages <- checkmate::makeAssertCollection()
  # check targetIds is a vector of int/double
  .checkCohortIds(
    cohortIds = targetIds,
    type = "target",
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

  # create list
  result <- list(
    targetIds = targetIds,
    limitToFirstInNDays = limitToFirstInNDays,
    minPriorObservation = minPriorObservation,
    covariateSettings = covariateSettings
  )

  class(result) <- "targetBaselineSettings"
  return(result)
}


computeTargetBaselineAnalyses <- function(
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cdmVersion = 5,
    targetDatabaseSchema,
    targetTable,
    characterizationDatabaseSchema,
    characterizationTable, # contains char cohorts
    #attritionTable,
    targetSettingsTable, # contains map between settings and char cohort id
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder,
    minCellCount = 0,
    progressBar = interactive(),
    minCharacterizationMean = 0.01,
    minCovariateCount = 0,
    executionId,
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  message("Target Baseline:  starting")

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # first look up the cohort ids for the settings
  cohorts <- lookupTargets(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = targetSettingsTable,
    tempEmulationSchema = tempEmulationSchema,
    targetIds = settings$targetIds,
    limitToFirstInNDays = settings$limitToFirstInNDays,
    minPriorObservation = settings$minPriorObservation
  )

  # next run FE on cohortIds
  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = characterizationDatabaseSchema,
    cohortTable = characterizationTable,
    cohortIds = unique(cohorts$characterizationTargetId),
    covariateSettings = ParallelLogger::convertJsonToSettings(settings$covariateSettings),
    cdmVersion = cdmVersion,
    aggregated = TRUE,
    minCharacterizationMean = minCharacterizationMean,
    tempEmulationSchema = tempEmulationSchema
  )

  # filter minCovariateCount
  if(minCovariateCount != 0){

    if(!is.null(result$covariates)){
      n <- result$covariates  %>% dplyr::count() %>% dplyr::pull()
      if(n > 0){
        result$covariates <- result$covariates %>% dplyr::filter(
          .data$sumValue >= !!minCovariateCount
        )
      }
    }

    if(!is.null(result$covariatesContinuous)){
      n <- result$covariatesContinuous  %>% dplyr::count() %>% dplyr::pull()
      if(n > 0){
        result$covariatesContinuous <- result$covariatesContinuous %>% dplyr::filter(
          .data$countValue >= !!minCovariateCount
        )
      }
    }

  }

  # rename the cohortDefinitionId column in covariates and covariatesContinuous
  if(!is.null(result$covariates)){
    result$covariates <- result$covariates %>%
      dplyr::rename("characterizationTargetId" = "cohortDefinitionId")
  }

  if(!is.null(result$covariatesContinuous)){
    result$covariatesContinuous <- result$covariatesContinuous %>%
      dplyr::rename("characterizationTargetId" = "cohortDefinitionId")
  }

  # rename
  result$targetCovariates <- result$covariates
  result$covariates <- NULL
  result$targetCovariatesContinuous <- result$covariatesContinuous
  result$covariatesContinuous <- NULL

  result$targetSettings <- cohorts

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

  message("Target Baseline:  ending")

  return(invisible(TRUE))

}

getTargetBaselineJobs <- function(
    characterizationSettings,
    nTargetJobs
    ) {

  characterizationSettings <- characterizationSettings$targetBaselineSettings
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
          result <- data.frame(
              targetIds = unique(characterizationSettings[[i]]$targetIds),
              limitToFirstInNDays = characterizationSettings[[i]]$limitToFirstInNDays,
              minPriorObservation = characterizationSettings[[i]]$minPriorObservation,
              covariateSettingsJson = as.character(ParallelLogger::convertSettingsToJson(characterizationSettings[[i]]$covariateSettings))
            )
            return(result)
        }
      )
  )

  settings <- c()
  if (nrow(targetCombinations) > 0) {
    jobCols <- c("targetIds")
    settingCols <- c("minPriorObservation", "limitToFirstInNDays")

    # thread split - assign each target a treat
    jobSettings <- targetCombinations %>%
      dplyr::select(dplyr::all_of(jobCols)) %>%
      dplyr::distinct()
    jobSettings$nTargetJobs <- rep(1:nTargetJobs, ceiling(nrow(jobSettings) / nTargetJobs))[1:nrow(jobSettings)]
    targetCombinations <- merge(targetCombinations, jobSettings, by = jobCols)

    executionSettings <- targetCombinations %>%
      dplyr::select(dplyr::all_of(settingCols)) %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        settingId = dplyr::row_number()
      )

    targetCombinations <- merge(targetCombinations, executionSettings, by = settingCols)

    # recreate settings
    for (settingId in unique(executionSettings$settingId)) {
      settingVal <- executionSettings %>%
        dplyr::filter(.data$settingId == !!settingId) %>%
        dplyr::select(dplyr::all_of(settingCols))

      restrictedData <- targetCombinations %>%
        dplyr::inner_join(settingVal, by = settingCols)

      for (i in unique(restrictedData$nTargetJobs)) {
        ind <- restrictedData$nTargetJobs== i
        settings <- rbind(
          settings,
          data.frame(
            functionName = "computeTargetBaselineAnalyses",
            settings = as.character(ParallelLogger::convertSettingsToJson(
              list(
                targetIds = unique(restrictedData$targetId[ind]),
                limitToFirstInNDays = unique(restrictedData$limitToFirstInNDays[ind]),
                minPriorObservation = unique(restrictedData$minPriorObservation[ind]),
                covariateSettingsJson = combineCovariateSettingsJsons(as.list(restrictedData$covariateSettingsJson[ind]))
              )
            )),
            executionFolder = paste("t", i, paste(settingVal, collapse = "_"), sep = "_"),
            jobId = paste("t", i, paste(settingVal, collapse = "_"), sep = "_")
          )
        )
      }
    }
  }

  return(settings)
}




