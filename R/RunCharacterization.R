#' Create the settings for a large scale characterization study
#' @description
#' This function creates a list of settings for different characterization studies
#'
#' @details
#' Specify one or more timeToEvent, dechallengeRechallenge and aggregateCovariate settings
#'
#' @param timeToEventSettings    A list of timeToEvent settings
#' @param dechallengeRechallengeSettings A list of dechallengeRechallenge settings
#' @param targetBaselineSettings A list of targetBaselineSettings settings
#' @param riskFactorSettings A list of riskFactorSettings settings
#' @param caseSeriesSettings A list of caseSeriesSettings settings
#' @family LargeScale
#'
#' @return
#' Returns the connection to the sqlite database
#'
#' @examples
#' # example code
#'
#' drSet <- createDechallengeRechallengeSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = 3
#' )
#'
#' cSet <- createCharacterizationSettings(
#'   dechallengeRechallengeSettings = drSet
#' )
#'
#' @export
createCharacterizationSettings <- function(
    timeToEventSettings = NULL,
    dechallengeRechallengeSettings = NULL,
    targetBaselineSettings = NULL,
    riskFactorSettings = NULL,
    caseSeriesSettings = NULL
    ) {

  errorMessages <- checkmate::makeAssertCollection()

  .checkTimeToEventSettingsList(
    settings = timeToEventSettings,
    errorMessages =  errorMessages
  )

  .checkDechallengeRechallengeSettingsList(
    settings = dechallengeRechallengeSettings,
    errorMessages = errorMessages
  )

  .checkTargetBaselineSettingsList(
    settings = targetBaselineSettings,
    errorMessages = errorMessages
  )

  .checkRiskFactorSettingsList(
    settings = riskFactorSettings,
    errorMessages = errorMessages
  )

  .checkCaseSeriesSettingsList(
    settings = caseSeriesSettings,
    errorMessages = errorMessages
  )

  if (inherits(timeToEventSettings, "timeToEventSettings")) {
    timeToEventSettings <- list(timeToEventSettings)
  }
  if (inherits(dechallengeRechallengeSettings, "dechallengeRechallengeSettings")) {
    dechallengeRechallengeSettings <- list(dechallengeRechallengeSettings)
  }
  if (inherits(targetBaselineSettings, "targetBaselineSettings")) {
    targetBaselineSettings <- list(targetBaselineSettings)
  }
  if (inherits(riskFactorSettings, "riskFactorSettings")) {
    riskFactorSettings <- list(riskFactorSettings)
  }
  if (inherits(caseSeriesSettings, "caseSeriesSettings")) {
    caseSeriesSettings <- list(caseSeriesSettings)
  }

  valid <- checkmate::reportAssertions(errorMessages)

  settings <- list(
    timeToEventSettings = timeToEventSettings,
    dechallengeRechallengeSettings = dechallengeRechallengeSettings,
    targetBaselineSettings = targetBaselineSettings,
    riskFactorSettings = riskFactorSettings,
    caseSeriesSettings = caseSeriesSettings
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
#' @family LargeScale
#'
#' @return
#' Returns the location of the directory containing the json settings
#'
#' @examples
#' drSet <- createDechallengeRechallengeSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = 3
#' )
#'
#' cSet <- createCharacterizationSettings(
#'   dechallengeRechallengeSettings = drSet
#' )
#'
#' saveCharacterizationSettings(
#'   settings = cSet,
#'   fileName = file.path(tempdir(), 'cSet.json')
#' )
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
#'
#' @family LargeScale
#'
#' @examples
#' # example code
#'
#' setPath <- file.path(tempdir(), 'charSet.json')
#'
#' drSet <- createDechallengeRechallengeSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = 3
#' )
#'
#' cSet <- createCharacterizationSettings(
#'   dechallengeRechallengeSettings = drSet
#' )
#'
#' saveCharacterizationSettings(
#'   settings = cSet,
#'   fileName = setPath
#' )
#'
#' setting <- loadCharacterizationSettings(setPath)
#'
#'
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
#' @param outputDatabaseSchema The schema where the characterization cohort table will be saved into
#' @param outputTable The table name where the characterization cohort table will be saved into
#' @param cdmDatabaseSchema The schema with the OMOP CDM data
#' @param characterizationSettings The study settings created using \code{createCharacterizationSettings}
#' @param outputDirectory The location to save the final csv files to
#' @param executionPath The location where intermediate results are saved to
#' @param csvFilePrefix A string to append the csv files in the outputDirectory
#' @param databaseId The unique identifier for the cdm database
#' @param showSubjectId  Whether to include subjectId of failed rechallenge case series or hide
#' @param minCellCount  The minimum count value that is calculated
#' @param incremental If TRUE then skip previously executed analyses that completed
#' @param threads    The number of threads to use when running in parallel
#' @param nTargetJobs Partition the targets into this number of groups (e.g., if there are 20 targets and njobs is 5 then there will be 4 targets per job and 5 jobs)
#' @param minCharacterizationMean The minimum mean threshold to extract when running aggregate covariates
#' @param minCovariateCount The minimum number of patients who must have the covariate when running aggregate covariates
#' @param mode Select from Efficient (no exclusions to target based on washout)/CohortIncidence (excludes targets with outcome in washout if they have no time at risk)/PatientLevelPrediction (excludes targets with outcome during washout prior to index)
#' @param minSMD The minimum standardized mean difference for the risk factor analysis
#' @family LargeScale
#'
#' @return
#' Multiple csv files in the outputDirectory.
#'
#' @examples
#'
#' conDet <- exampleOmopConnectionDetails()
#'
#' tteSet <- createTimeToEventSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = 3
#' )
#'
#' cSet <- createCharacterizationSettings(
#'   timeToEventSettings = tteSet
#' )
#'
#' runCharacterizationAnalyses(
#'   connectionDetails = conDet,
#'   targetDatabaseSchema = 'main',
#'   targetTable = 'cohort',
#'   outcomeDatabaseSchema = 'main',
#'   outcomeTable = 'cohort',
#'   cdmDatabaseSchema = 'main',
#'   characterizationSettings = cSet,
#'   outputDirectory = file.path(tempdir(),'runChar')
#' )
#'
#' @export
runCharacterizationAnalyses <- function(
    connectionDetails,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    outputDatabaseSchema = targetDatabaseSchema,
    outputTable = 'characterization_cohort',
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cdmDatabaseSchema,
    characterizationSettings,
    outputDirectory,
    executionPath = file.path(outputDirectory, "execution"),
    csvFilePrefix = "c_",
    databaseId = "1",
    showSubjectId = FALSE,
    minCellCount = 0,
    incremental = TRUE,
    threads = 1,
    nTargetJobs = 1,
    minCharacterizationMean = 0.01, # is this global or within cov set?
    minCovariateCount = 0, # is this global or within cov set?
    mode = 'CohortIncidence',
    minSMD = 0
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

  .checkConnectionDetails(
    connectionDetails = connectionDetails,
    errorMessages = errorMessages
  )

  checkmate::reportAssertions(
    errorMessages
  )
  # check model in c('Efficient', 'CohortIncidence', 'PatientLevelPrediction')
  if(!mode %in% c('Efficient', 'CohortIncidence', 'PatientLevelPrediction')){
    stop("Invalid mode.  Please select one of: 'Efficient', 'CohortIncidence', 'PatientLevelPrediction'")
  }

  runDateTime <- Sys.time()

  createDirectory(outputDirectory)
  createDirectory(executionPath)

  logger <- createLogger(
    logPath = file.path(executionPath),
    logName = "log.txt"
  )
  ParallelLogger::registerLogger(logger)
  on.exit(ParallelLogger::unregisterLogger(logger))

  # get execution id
  settingHash <-  digest::digest(
    object = as.character(characterizationSettings),
    algo = "md5",
    serialize = FALSE
  )
  executionId <- settingHash # for now using hash of json settings

  dbHash <-  digest::digest(
    object = as.character(cdmDatabaseSchema),
    algo = "md5",
    serialize = FALSE
  )

  jobs <- createJobs(
    characterizationSettings = characterizationSettings,
    nTargetJobs = nTargetJobs
  )

  # save settings
  if (!file.exists(file.path(executionPath, "settings.rds"))) {
    saveRDS(
      object = list(
        characterizationSettings = characterizationSettings,
        nTargetJobs = nTargetJobs,
        mode = mode
      ),
      file = file.path(executionPath, "settings.rds")
    )
  }

  # check settings are the same if running icremental
  if (incremental) {
    # check for any issues with current incremental
    oldSettings <- readRDS(
      file = file.path(executionPath, "settings.rds")
    )
    if (!identical(characterizationSettings, oldSettings$characterizationSettings)) {
      stop("Settings have changed - please turn off incremental")
    }
    if (!identical(nTargetJobs, oldSettings$nTargetJobs)) {
      stop("Cannot change nTargetJobs in incremental model")
    }
    if (!identical(mode, oldSettings$mode)) {
      stop(paste0("Cannot change mode in incremental model, please use ", oldSettings$mode, " mode."))
    }


    # create logs if not exists
    createIncrementalLog(
      executionFolder = executionPath,
      logname = "execution.csv"
    )
    createIncrementalLog(
      executionFolder = executionPath,
      logname = "completed.csv"
    )

    checkResultFilesIncremental(
      executionFolder = executionPath
    )

    # get all job paths (needed for export even if no jobs left)
    jobsExecutionFolder <- jobs$executionFolder

    # remove any previously completed jobs
    completedJobIds <- findCompletedJobs(executionFolder = executionPath)

    completedJobIndex <- jobs$jobId %in% completedJobIds
    if (sum(completedJobIndex) > 0) {
      message(paste0("Removing ", sum(completedJobIndex), " previously completed jobs"))
      jobs <- jobs[!completedJobIndex, ]
    }

    if (nrow(jobs) == 0) {
      message("No jobs left")
      exportAndromedaSubfilesToCsv(
        outputFolder = outputDirectory,
        executionPath = executionPath,
        csvFilePrefix = csvFilePrefix,
        minCellCount = minCellCount,
        batchSize = 100000
      )
      return(invisible(TRUE))
    }
  } else {
    # check for any csv files in folder
    checkResultFilesNonIncremental(
      executionFolder = executionPath
    )
  }


  # FIRST GENERATE ALL THE REQUIRED COHORTS - EXTRACT COHORT JOBS AND GENERATE
  tableNames <- generateCohorts(
    characterizationSettings = characterizationSettings,
    threads = threads,
    nTargetJobs = nTargetJobs,
    incremental = incremental,
    executionPath = executionPath,

    connectionDetails = connectionDetails,
    targetDatabaseSchema = targetDatabaseSchema,
    targetTable = targetTable,
    outcomeDatabaseSchema = outcomeDatabaseSchema,
    outcomeTable = outcomeTable,
    outputDatabaseSchema = outputDatabaseSchema,
    outputTable = outputTable,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    mode = mode,

    settingHash = settingHash,
    dbHash = dbHash
  )

  # extract attrition, case_settings, target_settings, execution_settings, case_series_settings
  exportSharedObjects(
    saveLocation = outputDirectory,
    executionPath = executionPath,
    tablePrefix = csvFilePrefix,
    executionId = settingHash,
    databaseId = databaseId,
    characterizationSettings = characterizationSettings,
    connectionDetails = connectionDetails,
    tempEmulationSchema = tempEmulationSchema,
    outputDatabaseSchema = outputDatabaseSchema,
    attritionTable = tableNames$attritionTable,
    targetSettingsTable = tableNames$targetSettingsTable,
    caseSettingsTable = tableNames$caseSettingsTable,
    dbHash = dbHash,
    mode = mode,
    minCharacterizationMean = minCharacterizationMean,
    minCovariateCount = minCovariateCount,
    minSMD = minSMD
  )

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
    minCovariateCount = minCovariateCount,
    executionPath = executionPath,
    incremental = incremental,

    # new inputs
    characterizationDatabaseSchema = outputDatabaseSchema,
    characterizationTable = tableNames$characterizationTable,
    targetSettingsTable = tableNames$targetSettingsTable,
    caseSettingsTable = tableNames$caseSettingsTable,
    mode = mode,
    minSMD = minSMD,
    executionId = executionId
  )



  # 2) convert jobList to list with extra inputs
  jobList <- lapply(
    X = 1:nrow(jobs),
    FUN = function(ind) {
      inputs <- inputSettings
      inputs$settings <- jobs$settings[ind]
      inputs$functionName <- jobs$functionName[ind]
      inputs$executionFolder <- jobs$executionFolder[ind]
      inputs$jobId <- jobs$jobId[ind]
      inputs$runDateTime <- runDateTime
      return(inputs)
    }
  )

  message("Creating new cluster")
  cluster <- ParallelLogger::makeCluster(
    numberOfThreads = threads,
    singleThreadToMain = TRUE,
    setAndromedaTempFolder = TRUE
  )

  ParallelLogger::clusterApply(
    cluster = cluster,
    x = jobList,
    fun = runCharacterizationsInParallel
  )

  # code to export all csvs into one file
  exportAndromedaSubfilesToCsv(
    executionPath = executionPath,
    outputFolder = outputDirectory,
    csvFilePrefix = csvFilePrefix,
    batchSize = 100000,
    minCellCount = minCellCount
  )
  exportAttrition(
    executionPath = executionPath,
    outputFolder = outputDirectory,
    csvFilePrefix = csvFilePrefix,
    minCellCount = minCellCount
  )

  invisible(outputDirectory)
}

createDirectory <- function(x) {
  if (!dir.exists(x)) {
    message(paste0("Creating directory ", x))
    dir.create(x, recursive = TRUE)
  }
}

createLogger <- function(logPath, logName) {
  createDirectory(logPath)
  ParallelLogger::createLogger(
    name = "Characterization",
    threshold = "INFO",
    appenders = list(
      ParallelLogger::createFileAppender(
        fileName = file.path(logPath, logName),
        layout = ParallelLogger::layoutParallel,
        expirationTime = 60 * 60 * 48
      )
    )
  )
}

runCharacterizationsInParallel <- function(x) {
  startTime <- Sys.time()

  functionName <- x$functionName
  inputSettings <- x
  inputSettings$functionName <- NULL
  inputSettings$settings <- ParallelLogger::convertJsonToSettings(inputSettings$settings)
  inputSettings$outputFolder <- file.path(x$executionPath, x$executionFolder)

  if (x$incremental) {
    recordIncremental(
      executionFolder = x$executionPath,
      runDateTime = x$runDateTime,
      jobId = x$jobId,
      startTime = startTime,
      endTime = startTime,
      logname = "execution.csv"
    )
  }

  completed <- tryCatch(
    {
      do.call(
        what = eval(parse(text = functionName)),
        args = inputSettings
      )
    },
    error = function(e) {
      rlang::inform(e$message)
      return(FALSE)
    }
  )

  endTime <- Sys.time()

  # if it completed without issues save it
  if (x$incremental & completed) {
    recordIncremental(
      executionFolder = x$executionPath,
      runDateTime = x$runDateTime,
      jobId = x$jobId,
      startTime = startTime,
      endTime = endTime,
      logname = "completed.csv"
    )
  }
}

createJobs <- function(
    characterizationSettings,
    nTargetJobs
    ) {
  jobDf <- rbind(

    getTimeToEventJobs(
      characterizationSettings,
      nTargetJobs
    ),
    getDechallengeRechallengeJobs(
      characterizationSettings,
      nTargetJobs
    ),
    getTargetBaselineJobs(
      characterizationSettings,
      nTargetJobs
    ),
    getRiskFactorJobs(
      characterizationSettings,
      nTargetJobs
    ),
    getCaseSeriesJobs(
      characterizationSettings,
      nTargetJobs
    )
  )

  return(jobDf)
}






exportSharedObjects <- function(
    saveLocation,
    executionPath,
    tablePrefix = '',
    executionId,
    databaseId,
    characterizationSettings,
    connectionDetails,
    outputDatabaseSchema,
    tempEmulationSchema,
    attritionTable,
    targetSettingsTable,
    caseSettingsTable,

    dbHash,
    mode,
    minCharacterizationMean,
    minCovariateCount,
    minSMD
){

  # add code here to save execution_settings,
  #      attrition, target_settings, case_settings and case_series_settings

  # connection
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))


  if(!dir.exists(saveLocation)){
    dir.create(saveLocation, recursive = TRUE)
  }

  # extract case series settings
 # getting global case series values
    if(is.null(characterizationSettings$caseSeriesSettings)){
      casePreTargetDuration = 0
      casePostOutcomeDuration = 0
    } else{
      casePreTargetDuration = max(unlist(lapply(
        X = characterizationSettings$caseSeriesSettings,
        FUN = function(x){
          x$casePreTargetDuration
        })))
      casePostOutcomeDuration = max(unlist(lapply(
        X = characterizationSettings$caseSeriesSettings,
        FUN = function(x){
          x$casePostOutcomeDuration
        })))
    }

  utils::write.csv(
    x = data.frame(
      setting_id = executionId,
      case_pre_target_duration = casePreTargetDuration,
      case_post_outcome_duration = casePostOutcomeDuration
    ),
    file = file.path(saveLocation, paste0(tablePrefix,'case_series_settings.csv')),
    row.names = FALSE
  )

  # extract attrition, target_settings
  if(!is.null(characterizationSettings$caseSeriesSettings) |
     !is.null(characterizationSettings$riskFactorSettings) |
     !is.null(characterizationSettings$targetBaselineSettings)){

    # export attrition table
    sql <- SqlRender::render(
      sql = "SELECT cohort_definition_id,	attr_reason, n FROM @attrition_table;",
      attrition_table = paste0(outputDatabaseSchema, '.' ,attritionTable)
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
    )

    andromeda <- Andromeda::andromeda()

    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      andromeda = andromeda,
      andromedaTableName = 'attrition',
      snakeCaseToCamelCase = TRUE
    )

    addDbAndSettings(
      andromeda = andromeda,
      databaseId = databaseId,
      settingId = executionId
    )

    saveCharacterizationAndromeda(
      andromeda = andromeda,
      outputFolder = file.path(executionPath, 'attrition')
    )

    # export target settings table
    sql <- SqlRender::render(
      sql = "SELECT target_id,	limit_to_first_in_n_days,	min_prior_observation, characterization_target_id FROM @target_settings_table;",
      target_settings_table = paste0(outputDatabaseSchema, '.' ,targetSettingsTable)
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
    )
    data <- DatabaseConnector::querySql(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = FALSE
    )
    data$database_id <- databaseId
    data$setting_id <- executionId
    utils::write.csv(
      x = data,
      file = file.path(saveLocation, paste0(tablePrefix,'target_settings.csv')),
      row.names = FALSE
    )

  }

  # extract case_settings
  if(!is.null(characterizationSettings$caseSeriesSettings) |
     !is.null(characterizationSettings$riskFactorSettings) ){

    # export target settings table
    sql <- SqlRender::render(
      sql = "SELECT characterization_case_id, characterization_target_id, outcome_id,	outcome_washout_days,	risk_window_start,	start_anchor,	risk_window_end,	end_anchor,	runtype	 FROM @case_settings_table;",
      case_settings_table = paste0(outputDatabaseSchema, '.' ,caseSettingsTable)
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema
    )
    data <- DatabaseConnector::querySql(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = FALSE
    )
    data$database_id <- databaseId
    data$setting_id <- executionId
    utils::write.csv(
      x = data,
      file = file.path(saveLocation, paste0(tablePrefix,'case_settings.csv')),
      row.names = FALSE
    )

  }

  # saving execution settings
  utils::write.csv(
    x = data.frame(
      setting_id = executionId,
      database_id = databaseId,
      database_hash = dbHash,
      mode = mode,
      min_characterization_mean = minCharacterizationMean,
      min_covariate_count = minCovariateCount,
      min_smd = minSMD
    ),
    file = file.path(saveLocation, paste0(tablePrefix,'execution_settings.csv')),
    row.names = FALSE
  )

  return(invisible(TRUE))
}


