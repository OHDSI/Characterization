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
    limitToFirstInNDays = 0,
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
    targetSettingsTable, # contains map between settings and char cohort id
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

  message("Target Baseline:  starting")

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # first look up the cohort ids for the settings
  cohorts <- lookupCohorts(
    connection = connection,
    lookupDatabaseSchema = characterizationDatabaseSchema,
    lookupTableName = targetSettingsTable,
    tempEmulationSchema = tempEmulationSchema,
    targetIds = settings$targetIds,
    limitToFirstInNDays = settings$limitToFirstInNDays,
    minPriorObservation = settings$minPriorObservation
  )

  cohorts$characterizationTargetId <- cohorts$characterizationTargetId*10

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
    minCharacterizationMean = settings$minCharacterizationMean,
    #minCharacterizationCount = settings$minCharacterizationCount,
    tempEmulationSchema = tempEmulationSchema
  )

  result$targetSettings <- cohorts

  # export all results to csv files
  message("Target Baseline: Exporting to csv")
  exportTargetAndromedaToCsv(
    andromeda = result,
    outputFolder = outputFolder,
    databaseId = databaseId,
    minCellCount = minCellCount
  )

  message("Target Baseline:  ending")

  return(invisible(TRUE))

}

getTargetBaselineJobs <- function(
    characterizationSettings,
    threads
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

  if (nrow(targetCombinations) > 0) {
    threadCols <- c("targetIds")
    settingCols <- c("minPriorObservation", "limitToFirstInNDays")

    # thread split - assign each target a treat
    threadSettings <- targetCombinations %>%
      dplyr::select(dplyr::all_of(threadCols)) %>%
      dplyr::distinct()
    threadSettings$thread <- rep(1:threads, ceiling(nrow(threadSettings) / threads))[1:nrow(threadSettings)]
    targetCombinations <- merge(targetCombinations, threadSettings, by = threadCols)

    executionSettings <- targetCombinations %>%
      dplyr::select(dplyr::all_of(settingCols)) %>%
      dplyr::distinct()

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
            functionName = "computeTargetBaselineAnalyses",
            settings = as.character(ParallelLogger::convertSettingsToJson(
              list(
                targetIds = unique(restrictedData$targetId[ind]),
                minPriorObservation = unique(restrictedData$minPriorObservation[ind]),
                covariateSettingsJson = combineCovariateSettingsJsons(as.list(restrictedData$covariateSettingsJson[ind])),
                settingId = settingId,
                limitToFirstInNDays = unique(restrictedData$limitToFirstInNDays[ind])
              )
            )),
            executionFolder = paste("t", i, paste(settingVal, collapse = "_"), sep = "_"),
            jobId = paste("t", i, paste(settingVal, collapse = "_"), sep = "_")
          )
        )
      }
    }
  } else {
    settings <- c()
  }

  return(settings)
}




exportTargetAndromedaToCsv <- function(
  andromeda,
  outputFolder,
  databaseId,
  minCellCount
){

  saveLocation <- outputFolder
  if (!dir.exists(saveLocation)) {
    dir.create(saveLocation, recursive = T)
  }

  # analysis_ref and covariate_ref
  # add database_id and setting_id
  if (!is.null(andromeda$analysisRef)) {
    Andromeda::batchApply(
      tbl = andromeda$analysisRef,
      fun = function(x) {
        data <- x #merge(x, ids)
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
        data <- x #merge(x, ids)
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


  if (!is.null(andromeda$covariates)) {
    Andromeda::batchApply(
      tbl = andromeda$covariates,
      fun = function(x) {
        data <- x #merge(x, extras, by = "cohortDefinitionId")
        #data <- data %>% dplyr::select(-"cohortDefinitionId")
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
        data <- x#merge(x, extras %>% dplyr::select(-"minCharacterizationMean"), by = "cohortDefinitionId")
        #data <- data %>% dplyr::select(-"cohortDefinitionId")
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

  # add targetSettings extraction

}

