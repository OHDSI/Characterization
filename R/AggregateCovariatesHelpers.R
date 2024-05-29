
hash <- function(x, seed=0){
  result <- digest::digest(
    object = x,
    algo='xxhash32',
    seed=seed
  )
  return(result)
}

extractCovariateList <- function(settings){
  result <- lapply(
    X = settings, function(x){
      x$covariateSettings
    })
  result <- unique(result)
  return(result)
}
extractCaseCovariateList <- function(settings){
  result <- lapply(
    X = settings, function(x){
      x$caseCovariateSettings
    })
  result <- unique(result)
  return(result)
}

extractCombinationSettings <- function(
    x,
    covariateSettingsList,
    caseCovariateSettingsList
    ){

  types <- data.frame(
    cohortType = c('TnO', 'OnT', 'TnObetween', 'TnOprior')
  )

  tars <- data.frame(
    riskWindowStart = x$riskWindowStart,
    startAnchor = x$startAnchor,
    riskWindowEnd = x$riskWindowEnd,
    endAnchor = x$endAnchor
  )

  # create hash of covariateSettings
  covariateSettingsHash <- hash(x$covariateSettings)
  covariateSettingsId <- which(unlist(lapply(covariateSettingsList, function(csl){
    identical(csl,x$covariateSettings)
  })))
  caseCovariateSettingsHash <- hash(x$caseCovariateSettings)
  caseCovariateSettingsId <- which(unlist(lapply(caseCovariateSettingsList, function(csl){
    identical(csl,x$caseCovariateSettings)
  })))

  # create T/O settings agnostic to TAR
  cohortDetails <- data.frame(
    targetCohortId = c(rep(x$targetIds, 2), rep(0,2*length(x$outcomeIds))),
    outcomeCohortId = c(rep(0,2*length(x$targetIds)), rep(x$outcomeIds, 2)),
    cohortType = c(
      rep('Tall', length(x$targetIds)),
      rep('Target', length(x$targetIds)),
      rep('Oall', length(x$outcomeIds)),
      rep('Outcome', length(x$outcomeIds))
    ),
    riskWindowStart = NA,
    startAnchor = NA,
    riskWindowEnd = NA,
    endAnchor = NA,
    minPriorObservation = x$minPriorObservation,
    outcomeWashoutDays = x$outcomeWashoutDays,
    covariateSettingsHash = covariateSettingsHash,
    covariateSettingsId = covariateSettingsId,
    caseCovariateSettingsHash = NA,
    caseCovariateSettingsId = NA,
    casePreTargetDuration = NA,
    casePostOutcomeDuration = NA
  )


  # full join T and O
  temp <- merge(
    x = x$targetIds,
    y = x$outcomeIds
  )

  #TnOprior <- data.frame(
  #  targetCohortId = temp$x,
  #  outcomeCohortId = temp$y,
  #  cohortType = 'TnOprior',
  #  riskWindowStart = NA,
  #  startAnchor = NA,
  #  riskWindowEnd = NA,
  #  endAnchor = NA,
  #  minPriorObservation = x$minPriorObservation,
  #  outcomeWashoutDays = x$outcomeWashoutDays,
  #  covariateSettingsHash = covariateSettingsHash,
  #  covariateSettingsId = covariateSettingsId,
  #  caseCovariateSettingsHash = NA,
  #  caseCovariateSettingsId = NA,
  #  casePreTargetDuration = NA,
  #  casePostOutcomeDuration = NA
  #)

  TnOs <- data.frame(
    targetCohortId = temp$x,
    outcomeCohortId = temp$y,
    minPriorObservation = x$minPriorObservation,
    outcomeWashoutDays = x$outcomeWashoutDays,
    covariateSettingsHash = covariateSettingsHash,
    covariateSettingsId = covariateSettingsId,
    caseCovariateSettingsHash = caseCovariateSettingsHash,
    caseCovariateSettingsId = caseCovariateSettingsId,
    casePreTargetDuration = x$casePreTargetDuration,
    casePostOutcomeDuration = x$casePostOutcomeDuration
  )

  TnOs <- TnOs %>% dplyr::cross_join(
    y = types
    ) %>% dplyr::cross_join(
      y = tars
    )

  combinations <- rbind(
    cohortDetails,
    #TnOprior,
    TnOs[,colnames(cohortDetails)]
    )

  return(combinations)
}

# specify the columns that impact O cohorts
getOSettingColumns <- function(){
  return(c('minPriorObservation','outcomeWashoutDays'))
}

# specify the columns that impact T cohorts
getTSettingColumns <- function(){
  return(c('runId','minPriorObservation'))
}

# specify the columns that impact case cohorts
getCaseSettingColumns <- function(){
  return(c('runId','minPriorObservation', 'outcomeWashoutDays',
    'casePreTargetDuration', 'casePostOutcomeDuration'#,
    #'riskWindowStart','startAnchor',
    #'riskWindowEnd','endAnchor'
  ))
}

createFolderName <- function(
  typeName,
  values
){

  if('startAnchor' %in% colnames(values)){
    values$startAnchor <- gsub(' ','_',values$startAnchor)
  }
  if('endAnchor' %in% colnames(values)){
    values$endAnchor <- gsub(' ','_',values$endAnchor)
  }

  folderNames <- unlist(
    lapply(
      1:nrow(values),
      function(i){
        paste(
          typeName,
          paste0(values[i,], collapse = '_' ), sep = '_'
        )
      }
    )
  )

  return(folderNames)
}

addFolderId <- function(
  cohortDetails,
  outputFolder,
  threads
){

  cohortDetails$folderId <- rep('', nrow(cohortDetails))

  # partition Ts in thread groups
  targetId <- unique(cohortDetails$targetCohortId)
  # add runId that splits Ts into thread runs
  cohortDetails <- merge(
    x = cohortDetails,
    y = data.frame(
      targetCohortId = targetId,
      runId = rep(
        1:threads,
        ceiling(length(targetId)/threads)
      )[1:length(targetId)]
    )
  )

  ind <- cohortDetails$cohortType %in% c('Target','Tall')
  cohortDetails$folderId[ind] <- createFolderName(
    typeName = 'T',
    values = cohortDetails[ind,getTSettingColumns()]
    )

  ind <- cohortDetails$cohortType %in% c('Outcome', 'Oall')
  cohortDetails$folderId[ind] <- createFolderName(
    typeName = 'O',
    values = cohortDetails[ind,getOSettingColumns()]
  )

  ind <- !cohortDetails$cohortType %in% c('Target','Tall', 'Outcome', 'Oall')
  cohortDetails$folderId[ind] <- createFolderName(
    typeName = 'cases',
    values = cohortDetails[ind,getCaseSettingColumns()]
  )

  # add the main output path
  cohortDetails$folderId <- sapply(cohortDetails$folderId, function(x){file.path(outputFolder, x)})

  return(cohortDetails)
}

incrementalColumns <- function(){
colNames <- c(
  'targetCohortId', 'outcomeCohortId', 'cohortType',
  'riskWindowStart','startAnchor',
  'riskWindowEnd','endAnchor',
  'minPriorObservation', 'outcomeWashoutDays',
  'covariateSettingsHash', 'caseCovariateSettingsHash',
  'covariateSettingsId', 'caseCovariateSettingsId',
  'casePreTargetDuration', 'casePostOutcomeDuration'
)
return(colNames )
}

removeExecuted <- function(
  cohortDetails,
  executedDetails
){

  colNames <- incrementalColumns()

  for(colName in colNames){
    class(executedDetails[,colName]) <- class(cohortDetails[,colName])
  }

  resultExists <- unlist(
    lapply(1:nrow(cohortDetails),
           function(i){
             sum(unlist(lapply(
               X = 1:nrow(executedDetails),
               FUN = function(j){
                 identical(
                   paste0(cohortDetails[i,colNames], collapse = '_'),
                   paste0(executedDetails[j,colNames], collapse = '_')
                 )
               }
             ))) > 0
           }
    )
  )

  message(paste0(sum(resultExists), ' analyses already generated'))
  message(paste0(sum(!resultExists), ' analyses left'))
  return(cohortDetails[!resultExists,])
}

saveIncremental <- function(
  cohortDetails,
  incrementalFile
){

  colNames <- incrementalColumns()

  if(file.exists(incrementalFile)){
    append <- T
  } else{
    append <- F
  }
  readr::write_csv(
    x = cohortDetails[,colNames],
    file = incrementalFile,
    append = append
  )

  return(invisible(NULL))
}

loadIncremental <- function(
    incrementalFile
){
  # check columns?
result <- read.csv(incrementalFile)
class(result$startAnchor) <- 'character'
class(result$endAnchor) <- 'character'
class(result$covariateSettingsHash) <- 'character'
class(result$caseCovariateSettingsHash) <- 'character'

return(result)
}

extractTargetFeatures <- function(
    inputList
    ){

  connectionDetails <- inputList$connectionDetails
  cdmDatabaseSchema <- inputList$cdmDatabaseSchema
  cohortDetails <- inputList$cohortDetails
  covariateSettingsList <- inputList$covariateSettingsList
  targetDatabaseSchema <- inputList$targetDatabaseSchema
  targetTable <- inputList$targetTable
  tempEmulationSchema <- inputList$tempEmulationSchema
  minCharacterizationMean <- inputList$minCharacterizationMean
  cdmVersion <- inputList$cdmVersion
  databaseId <- inputList$databaseId
  incrementalFile <- inputList$incrementalFile

  outputFolder <- cohortDetails$folderId[1]
  targetIds <- unique(cohortDetails$targetCohortId)
  minPriorObservation <- cohortDetails$minPriorObservation[1]
  covariateSettingsId <- cohortDetails$covariateSettingsId[1]
  covariateSettings <- covariateSettingsList[[covariateSettingsId]]

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # create the temp table with cohort_details
  DatabaseConnector::insertTable(
    data = cohortDetails[,c('targetCohortId','outcomeCohortId','cohortType','cohortDefinitionId')],
    camelCaseToSnakeCase = T,
    connection = connection,
    tableName =  '#cohort_details',
    tempTable = T,
    dropTableIfExists = T,
    createTable = T,
    progressBar = F
  )

  message("Computing aggregate target cohorts")
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
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  completionTime <- Sys.time() - start

  message(paste0('Computing target cohorts took ',round(completionTime,digits = 1), ' ', units(completionTime)))
  ## get counts
  message("Extracting target cohort counts")
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

  message("Computing aggregate target covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_before",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = covariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = minCharacterizationMean
  )

  # drop temp tables
  message("Dropping temp tables")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropTargetCovariate.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql, progressBar = FALSE,
    reportOverallTime = FALSE
  )

  # export all results to csv files
  exportAndromedaToCsv(
    andromeda = result,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = counts,
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean
  )

  if(!is.null(incrementalFile)){
    saveIncremental(
      cohortDetails = cohortDetails,
      incrementalFile = incrementalFile
    )
  }

}

extractOutcomeFeatures <- function(
    inputList
){

  connectionDetails <- inputList$connectionDetails
  cdmDatabaseSchema <- inputList$cdmDatabaseSchema
  cohortDetails <- inputList$cohortDetails
  covariateSettingsList <- inputList$covariateSettingsList
  outcomeDatabaseSchema <- inputList$outcomeDatabaseSchema
  outcomeTable <- inputList$outcomeTable
  tempEmulationSchema <- inputList$tempEmulationSchema
  minCharacterizationMean <- inputList$minCharacterizationMean
  cdmVersion <- inputList$cdmVersion
  databaseId <- inputList$databaseId
  incrementalFile <- inputList$incrementalFile

  outputFolder <- cohortDetails$folderId[1]
  outcomeIds <- unique(cohortDetails$outcomeCohortId)
  minPriorObservation <- cohortDetails$minPriorObservation[1]
  outcomeWashoutDays <- cohortDetails$outcomeWashoutDays[1]
  covariateSettingsId <- cohortDetails$covariateSettingsId[1]
  covariateSettings <- covariateSettingsList[[covariateSettingsId]]

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # create the temp table with cohort_details
  DatabaseConnector::insertTable(
    data = cohortDetails[,c('targetCohortId','outcomeCohortId','cohortType','cohortDefinitionId')],
    camelCaseToSnakeCase = T,
    connection = connection,
    tableName =  '#cohort_details',
    tempTable = T,
    dropTableIfExists = T,
    createTable = T,
    progressBar = F
  )

  message("Computing aggregate covariate outcome cohorts")
  start <- Sys.time()

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "OutcomeCohorts.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    cdm_database_schema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    outcome_database_schema = outcomeDatabaseSchema,
    outcome_table = outcomeTable,
    outcome_ids = paste(outcomeIds, collapse = ",", sep = ","),
    min_prior_observation = minPriorObservation,
    outcome_washout_days = outcomeWashoutDays
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  completionTime <- Sys.time() - start

  message(paste0('Computing outcome cohorts took ',round(completionTime,digits = 1), ' ', units(completionTime)))
  ## get counts
  message("Extracting outcome cohort counts")
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

  message("Computing aggregate outcome covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_before",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = covariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = minCharacterizationMean
  )

  # drop temp tables
  message("Dropping temp tables")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropOutcomeCovariate.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql, progressBar = FALSE,
    reportOverallTime = FALSE
  )

  # export all results to csv files
  exportAndromedaToCsv(
    andromeda = result,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = counts,
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean
  )

  # append to incremental file
  if(!is.null(incrementalFile)){
    saveIncremental(
      cohortDetails = cohortDetails,
      incrementalFile = incrementalFile
    )
  }

  return(invisible(NULL))
}


extractCaseFeatures <- function(
    inputList
){

  connectionDetails <- inputList$connectionDetails
  cdmDatabaseSchema <- inputList$cdmDatabaseSchema
  cohortDetails <- inputList$cohortDetails
  covariateSettingsList <- inputList$covariateSettingsList
  caseCovariateSettingsList <- inputList$caseCovariateSettingsList
  targetDatabaseSchema <- inputList$targetDatabaseSchema
  targetTable <- inputList$targetTable
  outcomeDatabaseSchema <- inputList$outcomeDatabaseSchema
  outcomeTable <- inputList$outcomeTable
  tempEmulationSchema <- inputList$tempEmulationSchema
  minCharacterizationMean <- inputList$minCharacterizationMean
  cdmVersion <- inputList$cdmVersion
  databaseId <- inputList$databaseId
  incrementalFile <- inputList$incrementalFile

  # get settings
  outputFolder <- cohortDetails$folderId[1]
  targetIds <- unique(cohortDetails$targetCohortId)
  outcomeIds <- unique(cohortDetails$outcomeCohortId)
  minPriorObservation <- cohortDetails$minPriorObservation[1]
  outcomeWashoutDays <- cohortDetails$outcomeWashoutDays[1]
  casePreTargetDuration <- cohortDetails$casePreTargetDuration[1]
  casePostOutcomeDuration <- cohortDetails$casePostOutcomeDuration[1]
  covariateSettingsId <- cohortDetails$covariateSettingsId[1]
  covariateSettings <- covariateSettingsList[[covariateSettingsId]]
  caseCovariateSettingsId <- cohortDetails$caseCovariateSettingsId[1]
  caseCovariateSettings <- caseCovariateSettingsList[[caseCovariateSettingsId]]

  tars <- unique(cohortDetails[, c('settingId','riskWindowStart','startAnchor', 'riskWindowEnd','endAnchor')])

  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # create the temp table with cohort_details
  DatabaseConnector::insertTable(
    data = cohortDetails[,c('targetCohortId','outcomeCohortId','cohortType','cohortDefinitionId', 'settingId')],
    camelCaseToSnakeCase = T,
    connection = connection,
    tableName =  '#cohort_details',
    tempTable = T,
    dropTableIfExists = T,
    createTable = T,
    progressBar = F
  )

  message("Computing aggregate case covariate cohorts")
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
    outcome_washout_days = outcomeWashoutDays#,
    #case_pre_target_duration = casePreTargetDuration,
    #case_post_target_duration = casePostTargetDuration
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  # loop over settingId which contains tars:
  for(i in 1:nrow(tars)){
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CaseCohortsPart2.sql",
      packageName = "Characterization",
      dbms = connectionDetails$dbms,
      #cdm_database_schema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      #target_database_schema = targetDatabaseSchema,
      #target_table = targetTable,
      #target_ids = paste(targetIds, collapse = ",", sep = ","),
      #outcome_database_schema = outcomeDatabaseSchema,
      #outcome_table = outcomeTable,
      #outcome_ids = paste(outcomeIds, collapse = ",", sep = ","),
      #min_prior_observation = minPriorObservation,
      #outcome_washout_days = outcomeWashoutDays,
      first = i==1,
      case_pre_target_duration = casePreTargetDuration,
      case_post_outcome_duration = casePostOutcomeDuration,
      setting_id = tars$settingId[i],
      tar_start = tars$riskWindowStart[i],
      tar_start_anchor = ifelse(tars$startAnchor[i] == 'cohort start','cohort_start_date','cohort_end_date'), ##TODO change?
      tar_end = tars$riskWindowEnd[i],
      tar_end_anchor = ifelse(tars$endAnchor[i] == 'cohort start','cohort_start_date','cohort_end_date') ##TODO change?
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  completionTime <- Sys.time() - start

  message(paste0('Computing case cohorts took ',round(completionTime,digits = 1), ' ', units(completionTime)))

  ## get counts
  message("Extracting case cohort counts")
  sql <- "select
  cohort_definition_id,
  count(*) row_count,
  count(distinct subject_id) person_count,
  min(datediff(day, cohort_start_date, cohort_end_date)) min_exposure_time,
  avg(datediff(day, cohort_start_date, cohort_end_date)) mean_exposure_time,
  max(datediff(day, cohort_start_date, cohort_end_date)) max_exposure_time
  from #agg_cohorts_before
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

  message("Computing aggregate before case covariate results")

  result <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_before",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = covariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = minCharacterizationMean
  )

  message("Computing aggregate during case covariate results")

  result2 <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = tempEmulationSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = "#agg_cohorts_cases",
    cohortTableIsTemp = T,
    cohortIds = -1,
    covariateSettings = caseCovariateSettings,
    cdmVersion = cdmVersion,
    aggregated = T,
    minCharacterizationMean = minCharacterizationMean
  )

  # drop temp tables
  message("Dropping temp tables")
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "DropCaseCovariate.sql",
    packageName = "Characterization",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql, progressBar = FALSE,
    reportOverallTime = FALSE
  )

  # export all results to csv files
  message("Exporting results to csv")
  exportAndromedaToCsv( # TODO combine export of result and result2
    andromeda = result,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = counts,
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean
  )
  exportAndromedaToCsv(
    andromeda = result2,
    outputFolder = outputFolder,
    cohortDetails = cohortDetails,
    counts = NULL, # previously added
    databaseId = databaseId,
    minCharacterizationMean = minCharacterizationMean
  )

  # append to incremental file
  if(!is.null(incrementalFile)){
    saveIncremental(
      cohortDetails = cohortDetails,
      incrementalFile = incrementalFile
    )
  }

  return(invisible(NULL))
}


exportAndromedaToCsv <- function(
  andromeda,
  outputFolder,
  cohortDetails,
  counts,
  databaseId,
  minCharacterizationMean,
  batchSize = 100000
){

  saveLocation <- outputFolder
  if(!dir.exists(saveLocation)){
    dir.create(saveLocation, recursive = T)
  }

  ids <- data.frame(
    settingId = unique(cohortDetails$settingId),
    databaseId = databaseId
  )

  # analysis_ref and covariate_ref
  # add database_id and setting_id
  Andromeda::batchApply(
    tbl =   andromeda$covariateRef,
    fun = function(x){
      data <- merge(x, ids)
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
      if(file.exists(file.path(saveLocation, 'covariate_ref.csv'))){
        append <- T
      } else{
        append = F
      }
      readr::write_csv(data, file = file.path(saveLocation, 'covariate_ref.csv'), append = append)
    },
    batchSize = batchSize
    )
  Andromeda::batchApply(
    tbl =   andromeda$analysisRef,
    fun = function(x){
      data <- merge(x, ids)
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
      if(file.exists(file.path(saveLocation, 'analysis_ref.csv'))){
        append <- T
      } else{
        append = F
      }
      readr::write_csv(data, file = file.path(saveLocation, 'analysis_ref.csv'), append = append)
    },
    batchSize = batchSize
    )

  # covariates and covariate_continuous
  extras <- cohortDetails[, c('cohortDefinitionId','settingId', 'targetCohortId', 'outcomeCohortId', 'cohortType')]
  extras$databaseId  <- databaseId
  extras$minCharacterizationMean <- minCharacterizationMean
  # add database_id, setting_id, target_cohort_id, outcome_cohort_id and cohort_type
  Andromeda::batchApply(
    tbl =   andromeda$covariates,
    fun = function(x){
      data <- merge(x, extras, by = 'cohortDefinitionId')
      data <- data %>% dplyr::select(-"cohortDefinitionId")
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
      if(file.exists(file.path(saveLocation, 'covariates.csv'))){
        append <- T
      } else{
        append = F
      }
      readr::write_csv(data, file = file.path(saveLocation, 'covariates.csv'), append = append)
    },
    batchSize = batchSize
  )
  Andromeda::batchApply(
    tbl =   andromeda$covariatesContinuous,
    fun = function(x){
      data <- merge(x, extras %>% dplyr::select(-"minCharacterizationMean"), by = 'cohortDefinitionId')
      data <- data %>% dplyr::select(-"cohortDefinitionId")
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
      if(file.exists(file.path(saveLocation, 'covariates_continuous.csv'))){
        append <- T
      } else{
        append = F
      }
      readr::write_csv(data, file = file.path(saveLocation, 'covariates_continuous.csv'), append = append)
    },
    batchSize = batchSize
  )

  # cohort_counts:
  if(!is.null(counts)){
    cohortCounts <- cohortDetails %>% dplyr::select(
      'targetCohortId',
      'outcomeCohortId',
      'cohortType',
      'cohortDefinitionId',
      'riskWindowStart',
      'riskWindowEnd',
      'startAnchor',
      'endAnchor',
      'minPriorObservation',
      'outcomeWashoutDays'
    ) %>%
      dplyr::mutate(
        databaseId = !!databaseId
      ) %>%
      dplyr::inner_join(counts, by = 'cohortDefinitionId') %>%
      dplyr::select(-"cohortDefinitionId")
    cohortCounts <- unique(cohortCounts)
    colnames(cohortCounts) <- SqlRender::camelCaseToSnakeCase(colnames(cohortCounts))
    if(file.exists(file.path(saveLocation, 'cohort_counts.csv'))){
      append <- T
    } else{
      append = F
    }
    readr::write_csv(cohortCounts, file = file.path(saveLocation, 'cohort_counts.csv'), append = append)
  }

  return(invisible(T))
}



aggregateCsvs <- function(
  outputFolder
){

  # this makes sure results are recreated
  firstTracker <- data.frame(
    table = c('covariates.csv','covariates_continuous.csv','covariate_ref.csv','analysis_ref.csv','cohort_counts.csv'),
    first = rep(T, 5)
  )

  analysisRefTracker <- c()
  covariateRefTracker <- c()

  mainFolder <- 'results'
  if(!dir.exists(file.path(outputFolder, mainFolder))){
    dir.create(file.path(outputFolder, mainFolder), recursive = T)
  }

  executionFolder <- file.path(outputFolder, 'execution')
  folderNames <- dir(executionFolder)

  # for each folder load covariates, covariates_continuous,
  # covariate_ref and analysis_ref
  for(folderName in folderNames){
    for(csvType in c('covariates.csv','covariates_continuous.csv','covariate_ref.csv','analysis_ref.csv','cohort_counts.csv')){

      loadPath <- file.path(executionFolder, folderName, csvType)
      savePath <- file.path(outputFolder, mainFolder, csvType)
      if(file.exists(loadPath)){

        #TODO do this in batches
        data <- readr::read_csv(
          file = loadPath,
          show_col_types = F
          )

        if(csvType == 'analysis_ref.csv'){
          data <- data %>%
            dplyr::filter(
              !.data$setting_id %in% analysisRefTracker
          )
          analysisRefTracker <- c(analysisRefTracker, unique(data$setting_id))
        }
        if(csvType == 'covariate_ref.csv'){
          data <- data %>%
            dplyr::filter(
              !.data$setting_id %in% covariateRefTracker
            )
          covariateRefTracker <- c(covariateRefTracker, unique(data$setting_id))
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

saveSettings <- function(
    outputFolder,
    cohortDetails,
    databaseId,
    covariateSettingsList,
    caseCovariateSettingsList
    ){

  saveLocation <- file.path(outputFolder, 'results')
  if(!dir.exists(saveLocation)){
    dir.create(saveLocation, recursive = T)
  }

  covariateSettingLookup <- data.frame(
    covariateSettingsId = 1:length(covariateSettingsList),
    covariateSettingJson = unlist(lapply(covariateSettingsList, function(x){ParallelLogger::convertSettingsToJson(x)})
  ))

  caseCovariateSettingLookup <- data.frame(
    caseCovariateSettingsId = 1:length(caseCovariateSettingsList),
    caseCovariateSettingJson = unlist(lapply(caseCovariateSettingsList, function(x){ParallelLogger::convertSettingsToJson(x)})
  ))


  settings <- cohortDetails %>%
    dplyr::select(
      'settingId',
      'minPriorObservation',
      'outcomeWashoutDays',
      'riskWindowStart',
      'riskWindowEnd',
      'startAnchor',
      'endAnchor',
      'casePreTargetDuration',
      'casePostOutcomeDuration',
      'covariateSettingsId',
      'caseCovariateSettingsId'
    ) %>%
    dplyr::left_join(
      covariateSettingLookup,
      by = 'covariateSettingsId'
    ) %>%
    dplyr::left_join(
      caseCovariateSettingLookup,
      by = 'caseCovariateSettingsId'
    ) %>%
    dplyr::mutate(
      databaseId = !!databaseId
    ) %>%
    dplyr::select(
      -'covariateSettingsId', -'caseCovariateSettingsId'
    )
  colnames(settings) <- SqlRender::camelCaseToSnakeCase(colnames(settings))
  settings <- unique(settings)
  readr::write_csv(settings, file = file.path(saveLocation, 'settings.csv'))

# cohort details: database_id, setting_id, target_cohort_id, outcome_cohort_id and cohort_type
  cd <- cohortDetails %>% dplyr::select(
    'settingId',
    'targetCohortId',
    'outcomeCohortId',
    'cohortType'
  ) %>%
    dplyr::mutate(
      databaseId = !!databaseId
    )
  cd <- unique(cd)
  colnames(cd) <- SqlRender::camelCaseToSnakeCase(colnames(cd))
  readr::write_csv(cd, file = file.path(saveLocation, 'cohort_details.csv'))
}
