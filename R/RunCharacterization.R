#' Create the settings for a large scale characterization study
#' @description
#' This function creates a list of settings for different characterization studies
#'
#' @details
#' Specify one or more timeToEvent, dechallengeRechallenge and aggregateCovariate settings
#'
#' @param timeToEventSettings    A list of timeToEvent settings
#' @param dechallengeRechallengeSettings A list of dechallengeRechallenge settings
#' @param aggregateCovariateSettings A list of aggregateCovariate settings
#' @family {LargeScale}
#' @return
#' Returns the connection to the sqlite database
#'
#' @export
createCharacterizationSettings <- function(
    timeToEventSettings = NULL,
    dechallengeRechallengeSettings = NULL,
    aggregateCovariateSettings = NULL
) {
  errorMessages <- checkmate::makeAssertCollection()
  .checkTimeToEventSettingsList(
    settings = timeToEventSettings,
    errorMessages =  errorMessages
  )
  .checkAggregateCovariateSettingsList(
    settings = aggregateCovariateSettings,
    errorMessages = errorMessages
  )
  .checkDechallengeRechallengeSettingsList(
    settings = dechallengeRechallengeSettings,
    errorMessages = errorMessages
  )

  if (inherits(timeToEventSettings, "timeToEventSettings")) {
    timeToEventSettings <- list(timeToEventSettings)
  }
  if (inherits(dechallengeRechallengeSettings, "dechallengeRechallengeSettings")) {
    dechallengeRechallengeSettings <- list(dechallengeRechallengeSettings)
  }
  if (inherits(aggregateCovariateSettings, "aggregateCovariateSettings")) {
    aggregateCovariateSettings <- list(aggregateCovariateSettings)
  }

  valid <- checkmate::reportAssertions(errorMessages)

  settings <- list(
    timeToEventSettings = timeToEventSettings,
    dechallengeRechallengeSettings = dechallengeRechallengeSettings,
    aggregateCovariateSettings = aggregateCovariateSettings
  )

  class(settings) <- "characterizationSettings"

  return(settings)
}


#' Save the characterization settings as a json
#' @description
#' This function converts the settings into a json object and saves it
#'
#' @details
#' Input the characterization settings and output a json file to a file named 'characterizationSettings.json' inside the saveDirectory
#'
#' @param settings    An object of class characterizationSettings created using \code{createCharacterizationSettings}
#' @param fileName  The location to save the json settings
#' @family {LargeScale}
#' @return
#' Returns the location of the directory containing the json settings
#'
#' @export
saveCharacterizationSettings <- function(
    settings,
    fileName) {
  ParallelLogger::saveSettingsToJson(
    object = settings,
    fileName = fileName
  )

  invisible(fileName)
}

#' Load the characterization settings previously saved as a json file
#' @description
#' This function converts the json file back into an R object
#'
#' @details
#' Input the directory containing the 'characterizationSettings.json' file and load the settings into R
#'
#' @param fileName  The location of the the json settings
#'
#' @return
#' Returns the json settings as an R object
#' @family {LargeScale}
#' @export
loadCharacterizationSettings <- function(
    fileName) {
  settings <- ParallelLogger::loadSettingsFromJson(
    fileName = fileName
  )

  return(settings)
}

#' execute a large-scale characterization study
#' @description
#' Specify the database connection containing the CDM data, the cohort database schemas/tables,
#' the characterization settings and the directory to save the results to
#'
#' @details
#' The results of the characterization will be saved into an sqlite database inside the
#' specified saveDirectory
#'
#' @param connectionDetails  The connection details to the database containing the OMOP CDM data
#' @template TargetOutcomeTables
#' @template TempEmulationSchema
#' @param cdmDatabaseSchema The schema with the OMOP CDM data
#' @param characterizationSettings The study settings created using \code{createCharacterizationSettings}
#' @param outputDirectory The location to save the final csv files to
#' @param executionPath The location where intermediate results are saved to
#' @param csvFilePrefix A string to append the csv files in the outputDirectory
#' @param databaseId The unique identifier for the cdm database
#' @param showSubjectId  Whether to include subjectId of failed rechallenge case series or hide
#' @param minCellCount  The minimum count value that is calculated
#' @param incremental If TRUE then skip previously executed analyses that completed
#' @param threads    The number of threads to use when running aggregate covariates
#' @param minCharacterizationMean The minimum mean threshold to extract when running aggregate covariates
#' @family {LargeScale}
#' @return
#' Multiple csv files in the outputDirectory.
#'
#' @export
runCharacterizationAnalyses <- function(
    connectionDetails,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cdmDatabaseSchema,
    characterizationSettings,
    outputDirectory,
    executionPath = file.path(outputDirectory, 'execution'),
    csvFilePrefix = "c_",
    databaseId = "1",
    showSubjectId = F,
    minCellCount = 0,
    incremental = T,
    threads = 1,
    minCharacterizationMean = 0.01
) {
  # inputs checks
  errorMessages <- checkmate::makeAssertCollection()
  .checkCharacterizationSettings(
    settings = characterizationSettings,
    errorMessages = errorMessages
  )
  .checkTablePrefix(
    tablePrefix = csvFilePrefix,
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(
    errorMessages
  )

  runDateTime <- Sys.time()

  createDirectory(outputDirectory)
  createDirectory(executionPath)

  logger <- createLogger(
    logPath = file.path(executionPath),
    logName = 'log.txt'
  )
  ParallelLogger::registerLogger(logger)
  on.exit(ParallelLogger::unregisterLogger(logger))

  jobs <- createJobs(
    characterizationSettings = characterizationSettings,
    threads = threads
    )

  # save settings
  if(!file.exists(file.path(executionPath, 'settings.rds'))){
    saveRDS(
      object = list(
        characterizationSettings = characterizationSettings,
        threads = threads
        ),
      file = file.path(executionPath, 'settings.rds')
    )
  }

  if(incremental){
    # check for any issues with current incremental
    oldSettings <- readRDS(
      file = file.path(executionPath, 'settings.rds')
      )
    if(!identical(characterizationSettings,oldSettings$characterizationSettings)){
      stop('Settings have changed - please turn off incremental')
    }
    if(!identical(threads,oldSettings$threads)){
      stop('Cannot change number of threads in incremental model')
    }

    # create logs if not exists
    createIncrementalLog(
      executionFolder = executionPath,
      logname = 'execution.csv'
    )
    createIncrementalLog(
      executionFolder = executionPath,
      logname = 'completed.csv'
    )

    checkResultFilesIncremental(
      executionFolder  = executionPath
    )

    # remove any previously completed jobs
    completedJobIds <- findCompletedJobs(executionFolder = executionPath)

    completedJobIndex <- jobs$jobId %in% completedJobIds
    if(sum(completedJobIndex) > 0){
      message(paste0('Removing ', sum(completedJobIndex), ' previously completed jobs'))
      jobs <- jobs[!completedJobIndex,]
    }

    if(nrow(jobs) == 0){
      message('No jobs left')
      return(invisible(T))
    }

  } else{
    # check for any csv files in folder
    checkResultFilesNonIncremental(
      executionFolder = executionPath
    )
  }



  # Now loop over the jobs
  inputSettings <- list(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = targetDatabaseSchema,
    targetTable = targetTable,
    outcomeDatabaseSchema = outcomeDatabaseSchema,
    outcomeTable = outcomeTable,
    tempEmulationSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    databaseId = databaseId,
    showSubjectId = showSubjectId,
    minCellCount = minCellCount,
    minCharacterizationMean = minCharacterizationMean,
    executionPath = executionPath,
    incremental = incremental
  )

  # convert jobList to list with extra inputs
  jobList <- lapply(
    X = 1:nrow(jobs),
    FUN = function(ind){
      inputs <- inputSettings
      inputs$settings <- jobs$settings[ind]
      inputs$functionName <- jobs$functionName[ind]
      inputs$executionFolder<- jobs$executionFolder[ind]
      inputs$jobId <-jobs$jobId[ind]
      inputs$runDateTime <- runDateTime
      return(inputs)
    })

  message('Creating new cluster')
  cluster <- ParallelLogger::makeCluster(
    numberOfThreads = threads,
    singleThreadToMain = T,
    setAndromedaTempFolder = T
  )

  ParallelLogger::clusterApply(
    cluster = cluster,
    x = jobList,
    fun = runCharacterizationsInParallel
  )

  # code to export all csvs into one file
  aggregateCsvs(
    outputFolder = outputDirectory,
    executionPath = executionPath,
    executionFolders = jobs$executionFolder,
    csvFilePrefix = csvFilePrefix
  )

  invisible(outputDirectory)
}

createDirectory <- function(x){
  if(!dir.exists(x)){
    message(paste0('Creating directory ', x))
    dir.create(x, recursive = T)
  }
}

createLogger <- function(logPath, logName){
  createDirectory(logPath)
  ParallelLogger::createLogger(
    name = 'Characterization',
    threshold = "INFO",
    appenders = list(
      ParallelLogger::createFileAppender(
      fileName = file.path(logPath, logName),
      layout = ParallelLogger::layoutParallel,
      expirationTime = 60*60*48
    )
    )
  )
}

runCharacterizationsInParallel <- function(x){

  startTime <- Sys.time()

  functionName <- x$functionName
  inputSettings <- x
  inputSettings$functionName <- NULL
  inputSettings$settings <- ParallelLogger::convertJsonToSettings(inputSettings$settings)
  inputSettings$outputFolder <- file.path(x$executionPath, x$executionFolder)

    if(x$incremental){
      recordIncremental(
        executionFolder = x$executionPath,
        runDateTime = x$runDateTime,
        jobId = x$jobId,
        startTime = startTime,
        endTime = startTime,
        logname = 'execution.csv'
      )
    }

  completed <- tryCatch(
    {
      do.call(
        what = eval(parse(text = functionName)),
        args = inputSettings
      )
    }, error = function(e){print(e); return(FALSE)}

    )

  endTime <- Sys.time()

    # if it completed without issues save it
    if(x$incremental & completed){
      recordIncremental(
        executionFolder = x$executionPath,
        runDateTime = x$runDateTime,
        jobId = x$jobId,
        startTime = startTime,
        endTime = endTime,
        logname = 'completed.csv'
      )
    }
}

createJobs <- function(
  characterizationSettings,
  threads
){

  jobDf <- rbind(
    getTimeToEventJobs(
      characterizationSettings,
      threads
    ),
    getDechallengeRechallengeJobs(
      characterizationSettings,
      threads
    ),
    getAggregateCovariatesJobs(
        characterizationSettings,
        threads
    )
  )

  #data.frame(
  #  functionName,
  #  settings # json,
  #  executionFolder,
  #  jobId
  #)

  return(jobDf)
}



aggregateCsvs <- function(
    executionPath,
    outputFolder,
    executionFolders, # needed?
    csvFilePrefix
){

  tables <- c('cohort_details.csv', 'settings.csv','covariates.csv',
              'covariates_continuous.csv','covariate_ref.csv',
              'analysis_ref.csv','cohort_counts.csv',
              'time_to_event.csv',
              'rechallenge_fail_case_series.csv', 'dechallenge_rechallenge.csv')

  # this makes sure results are recreated
  firstTracker <- data.frame(
    table = tables,
    first = rep(T, length(tables))
  )

  analysisRefTracker <- c()
  covariateRefTracker <- c()
  settingsTracker <- c()

  # create outputFolder

  folderNames <- dir(executionPath)

  # for each folder load covariates, covariates_continuous,
  # covariate_ref and analysis_ref
  for(folderName in folderNames){
    for(csvType in tables){

      loadPath <- file.path(executionPath, folderName, csvType)
      savePath <- file.path(outputFolder, paste0(csvFilePrefix,csvType))
      if(file.exists(loadPath)){

        #TODO do this in batches
        data <- readr::read_csv(
          file = loadPath,
          show_col_types = F
        )

        if(csvType == 'analysis_ref.csv'){
          data <- data %>%
            dplyr::mutate(
              unique_id = paste0(.data$setting_id, '-', .data$analysis_id)
            ) %>%
            dplyr::filter( # need to filter analysis_id and setting_id
              !.data$unique_id %in% analysisRefTracker
            ) %>%
            dplyr::select(-"unique_id")

          analysisRefTracker <- unique(c(analysisRefTracker, paste0(data$setting_id,'-',data$analysis_id)))
        }
        if(csvType == 'covariate_ref.csv'){ # this could be problematic as may have differnet covariate_ids
          data <- data %>%
            dplyr::mutate(
              unique_id = paste0(.data$setting_id, '-', .data$covariate_id)
            ) %>%
            dplyr::filter( # need to filter covariate_id and setting_id
              !.data$unique_id %in% covariateRefTracker
            )%>%
            dplyr::select(-"unique_id")

          covariateRefTracker <- unique(c(covariateRefTracker, paste0(data$setting_id,'-',data$covariate_id)))
        }
        if(csvType == 'settings.csv'){
          data <- data %>%
            dplyr::filter(
              !.data$setting_id %in% settingsTracker
            )
          settingsTracker <- c(settingsTracker, unique(data$setting_id))
        }

        append <- file.exists(savePath)
        readr::write_csv(
          x = data,
          file = savePath, quote = 'all',
          append = append & !firstTracker$first[firstTracker$table == csvType]
        )
        firstTracker$first[firstTracker$table == csvType] <- F
      }

    }
  }
}
