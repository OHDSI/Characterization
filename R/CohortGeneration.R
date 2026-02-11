generateCohorts <- function(
    characterizationSettings,
    threads = 1,
    mode,
    incremental,
    executionPath = executionPath,
    connectionDetails,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    outputDatabaseSchema = targetDatabaseSchema,
    outputTable = 'characterization_cohort',
    cdmDatabaseSchema,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    progressBar = interactive()
){

  # connection
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  settingHash <-  digest::digest(
    object = as.character(characterizationSettings),
    algo = "md5",
    serialize = FALSE
    )

  dbHash <-  digest::digest(
    object = as.character(cdmDatabaseSchema),
    algo = "md5",
    serialize = FALSE
  )

  # tables names
  characterizationTableWithHash <- paste0(outputTable, '_',settingHash, '_', dbHash)
  targetSettingsTableWithHash <- paste0('target_settings', '_',settingHash, '_', dbHash)
  caseSettingsTableWithHash <- paste0('case_settings', '_',settingHash, '_', dbHash)
  attributeTableWithHash <- paste0('attributes', '_',settingHash, '_', dbHash)

  cohortJobs <- getCohortJobs(
    characterizationSettings,
    mode = mode
  )

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

  # upload settings:
  # 1) target_settings cohortJobs$targets
  if(!is.null(cohortJobs$targets)){
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = outputDatabaseSchema,
      tableName = targetSettingsTableWithHash,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      data = cohortJobs$targets,
      camelCaseToSnakeCase = TRUE
    )
  }

  # 2) case_settings cohortJobs$cases
  if(!is.null(cohortJobs$cases)){
    DatabaseConnector::insertTable(
      connection = connection,
      databaseSchema = outputDatabaseSchema,
      tableName = caseSettingsTableWithHash,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      data = cohortJobs$cases,
      camelCaseToSnakeCase = TRUE
    )
  }

  if(incremental){

    if(!file.exists(file.path(executionPath,'cohort_job_tracker.csv'))){
      sql <- SqlRender::loadRenderTranslateSql(
        sqlFilename = 'CreateTargetCohortTable.sql',
        packageName = 'Characterization',
        tempEmulationSchema = tempEmulationSchema,
        characterization_schema = outputDatabaseSchema,
        characterization_table = characterizationTableWithHash,
        attrition_table = attributeTableWithHash
      )

      DatabaseConnector::executeSql(connection, sql)

      tracker <- data.frame(
        jobId = 'targetTableCreate',
        completeDate = date()
      )

      if(!dir.exists(executionPath)){
        dir.create(executionPath, recursive = TRUE)
      }

      readr::write_csv(
        file = file.path(executionPath,'cohort_job_tracker.csv'),
        x = tracker
      )

    } else{
      tracker <- read.csv(file.path(executionPath,'cohort_job_tracker.csv'))
    }

    completedJobIndex <- cohortJobs$jobs$jobId %in% tracker$jobId
    if (sum(completedJobIndex) > 0) {
      message(paste0("Removing ", sum(completedJobIndex), " previously completed cohort jobs"))
      cohortJobs$jobs <- cohortJobs$jobs[!completedJobIndex, ]
    }


  } else{

    # create the characterization cohort table
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = 'CreateTargetCohortTable.sql',
      packageName = 'Characterization',
      tempEmulationSchema = tempEmulationSchema,
      characterization_schema = outputDatabaseSchema,
      characterization_table = characterizationTableWithHash,
      attrition_table = attributeTableWithHash
      )

    DatabaseConnector::executeSql(connection, sql)

  }

  # TODO: replace below with parallel jobs using threads
  for(i in 1:nrow(cohortJobs$jobs)){
    do.call(
      what = cohortJobs$jobs$functionName[i],
      args =
        list(
          connection = connection,
          cdmDatabaseSchema = cdmDatabaseSchema,
          characterizationTable = characterizationTableWithHash,
          attritionTable = attributeTableWithHash,
          targetSettingsTable = targetSettingsTableWithHash,
          caseSettingsTable = caseSettingsTableWithHash,
          characterizationDatabaseSchema = outputDatabaseSchema,
          tempEmulationSchema = tempEmulationSchema,
          targetDatabaseSchema = targetDatabaseSchema,
          targetTable = targetTable,
          outcomeDatabaseSchema = outcomeDatabaseSchema,
          outcomeTable = outcomeTable,
          incremental = incremental,
          mode = mode,

          casePreTargetDuration = casePreTargetDuration,
          casePostOutcomeDuration = casePostOutcomeDuration,

          progressBar = progressBar,
          executionPath = executionPath,
          settings = ParallelLogger::convertJsonToSettings(cohortJobs$jobs$settings[i]),
          jobId = cohortJobs$jobs$jobId[i]
        )
      )
  }

return(list(
  characterizationTable = characterizationTableWithHash,
  targetSettingsTable = targetSettingsTableWithHash,
  caseSettingsTable = caseSettingsTableWithHash,
  attritionTable = attributeTableWithHash
)
)
}



# make this return data.frame with columns functionName/settings/executionFolder/jobId
# where settings into functionName is a json: as.character(ParallelLogger::convertSettingsToJson(list))
# partitions by targetId (thread) and settings
getCohortJobs <- function(
    characterizationSettings,
    mode,
    threads # not currently used
){

  targets <- c()
  cases <- c()

  # Extracting Target Baseline targets
  if(!is.null(characterizationSettings$targetBaselineSettings)){

    tempTargets <- do.call(
      what = 'rbind',
      args = lapply(
      X = characterizationSettings$targetBaselineSettings,
      FUN = function(x){
        data.frame(
          targetId = x$targetIds,
          limitToFirstInNDays = x$limitToFirstInNDays,
          minPriorObservation = x$minPriorObservation
        )
      }
    )
    )

    targets <- rbind(
      targets,
      tempTargets
    )

  }


  # Extracting Risk Factor targets and cases
  if(!is.null(characterizationSettings$riskFactorSettings)){

    tempTargets <- do.call(
      what = 'rbind',
      args = lapply(
        X = characterizationSettings$riskFactorSettings,
        FUN = function(x){
          data.frame(
            targetId = x$targetIds,
            limitToFirstInNDays = x$limitToFirstInNDays,
            minPriorObservation = x$minPriorObservation
          )
        }
      )
    )

    targets <- rbind(
      targets,
      tempTargets
    )

    tempCases <- do.call(
      what = 'rbind',
      args = lapply(
        X = characterizationSettings$riskFactorSettings,
        FUN = function(x){
          do.call(
            what = 'rbind',
            lapply(
              X = unique(x$targetIds),
              FUN = function(y){
                data.frame(
                  targetId = y,
                  limitToFirstInNDays = x$limitToFirstInNDays,
                  minPriorObservation = x$minPriorObservation,
                  outcomeId = x$outcomeIds,
                  outcomeWashoutDays = x$outcomeWashoutDays,
                  riskWindowStart = x$riskWindowStart,
                  startAnchor = x$startAnchor,
                  riskWindowEnd = x$riskWindowEnd,
                  endAnchor = x$endAnchor,
                  type = 'risk-factor'
                )
              }
            )
          )
        }
      )
    )

    cases <- rbind(
      cases,
      tempCases
    )

  }

  # Extracting Case Series cases
  if(!is.null(characterizationSettings$caseSeriesSettings)){

    tempTargets <- do.call(
      what = 'rbind',
      args = lapply(
        X = characterizationSettings$targetBaselineSettings,
        FUN = function(x){
          data.frame(
            targetId = x$targetIds,
            limitToFirstInNDays = x$limitToFirstInNDays,
            minPriorObservation = x$minPriorObservation
          )
        }
      )
    )

    targets <- rbind(
      targets,
      tempTargets
    )

    tempCases <- do.call(
      what = 'rbind',
      args = lapply(
        X = characterizationSettings$riskFactorSettings,
        FUN = function(x){
          do.call(
            what = 'rbind',
            lapply(
              X = unique(x$targetIds),
              FUN = function(y){
                data.frame(
                  targetId = y,
                  limitToFirstInNDays = x$limitToFirstInNDays,
                  minPriorObservation = x$minPriorObservation,
                  outcomeId = x$outcomeIds,
                  outcomeWashoutDays = x$outcomeWashoutDays,
                  riskWindowStart = x$riskWindowStart,
                  startAnchor = x$startAnchor,
                  riskWindowEnd = x$riskWindowEnd,
                  endAnchor = x$endAnchor,
                  type = 'case-series'
                )
              }
            )
          )
        }
      )
    )

    cases <- rbind(
      cases,
      tempCases
    )

  }

  jobs <- c()

  if(!is.null(nrow(targets))){

    targets <- unique(targets) %>%
      dplyr::inner_join(
        y = targets %>%
          dplyr::distinct(.data$limitToFirstInNDays, .data$minPriorObservation) %>%
          dplyr::arrange(.data$limitToFirstInNDays, .data$minPriorObservation) %>%
          dplyr::mutate(
            settingId = dplyr::row_number()
          ),
        by = c("limitToFirstInNDays", "minPriorObservation")
      ) %>%
      dplyr::mutate(
        characterizationTargetId = dplyr::row_number()*10
      )


    message(paste0('Adding ', length(unique(targets$settingId)) ,' Target Baseline Jobs'))

    for(setId in unique(targets$settingId)){
      toi <- targets %>%
        dplyr::filter(.data$settingId == !!setId)

      jobs <- rbind(jobs,data.frame(
        functionName = 'generateTargets',
        settings =  as.character(ParallelLogger::convertSettingsToJson(
          list(
            settingId = setId,
            targetIds = unique(toi$targetId),
            limitToFirstInNDays = unique(toi$limitToFirstInNDays),
            minPriorObservation = unique(toi$minPriorObservation)
          ))),
      jobId = paste("targets_", setId, sep = "_")
      ))

    }
  }


  if(!is.null(nrow(cases))){
    cases <- unique(cases) %>%
      dplyr::inner_join(
        y = cases %>%
          dplyr::distinct(.data$outcomeWashoutDays, .data$outcomeId,
                          .data$riskWindowStart,.data$startAnchor,
                          .data$riskWindowEnd, .data$endAnchor) %>%
          dplyr::arrange(.data$outcomeWashoutDays, .data$outcomeId,
                         .data$riskWindowStart,.data$startAnchor,
                         .data$riskWindowEnd, .data$endAnchor) %>%
          dplyr::mutate(
            settingId = dplyr::row_number()
          ),
        by = c("outcomeWashoutDays", "outcomeId",
               "riskWindowStart", "startAnchor",
               "riskWindowEnd", "endAnchor")
      ) %>%
      dplyr::inner_join(
        y = targets %>%
          dplyr::select("characterizationTargetId", "targetId","limitToFirstInNDays", "minPriorObservation"),
        by = c("targetId","limitToFirstInNDays", "minPriorObservation")
      ) %>%
      dplyr::select(-"targetId",-"limitToFirstInNDays", -"minPriorObservation") %>%
      dplyr::mutate(
        characterizationCaseId = dplyr::row_number()
      )

    message(paste0('Adding ', length(unique(cases$settingId)) ,' Case Jobs'))

    for(setId in unique(cases$settingId)){
      coi <- cases %>%
        dplyr::filter(.data$settingId == !!setId)

      jobs <- rbind(jobs, data.frame(
        functionName = 'generateCases',
        settings = as.character(ParallelLogger::convertSettingsToJson(list(
          settingId = setId,
          analysisId = 2,
          characterizationTargetIds = unique(coi$characterizationTargetId),
          outcomeIds = unique(coi$outcomeId),
          outcomeWashoutDays = unique(coi$outcomeWashoutDays),
          riskWindowStart = unique(coi$riskWindowStart),
          startAnchor = unique(coi$startAnchor),
          riskWindowEnd = unique(coi$riskWindowEnd),
          endAnchor = unique(coi$endAnchor),
          generateRiskFactors = 'risk-factor' %in% unique(coi$type),
          generateCaseSeries = 'case-series' %in% unique(coi$type)
        )
      )),
      jobId = paste("cases_", setId, sep = "_")
      ))

      if(mode != 'Efficient'){
      if('risk-factor' %in% unique(coi$type)){
        message(paste0('Adding ', length(unique(cases$settingId)) ,' Non Case Jobs'))
        jobs <- rbind(jobs, data.frame(
          functionName = 'generateNonCases',
          settings = as.character(ParallelLogger::convertSettingsToJson(list(
            settingId = setId,
            characterizationTargetIds = unique(coi$characterizationTargetId),
            outcomeIds = unique(coi$outcomeId),
            outcomeWashoutDays = unique(coi$outcomeWashoutDays),
            riskWindowStart = unique(coi$riskWindowStart),
            startAnchor = unique(coi$startAnchor),
            riskWindowEnd = unique(coi$riskWindowEnd),
            endAnchor = unique(coi$endAnchor)
          )
          )),
          jobId = paste("non_cases_", setId, sep = "_")
        ))
      }

    } # not efficient
    }
  }

  return(
    list(
      targets = targets,
      cases = cases,
      jobs = jobs
    )
  )
}



generateTargets <- function(
    connection,
    cdmDatabaseSchema,
    characterizationTable,
    attritionTable,
    targetSettingsTable,
    characterizationDatabaseSchema,
    tempEmulationSchema,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    progressBar = interactive(),
    executionPath,
    settings,
    jobId,
    mode,
    incremental,
    ...
){

  message("Creating Target Cohorts")
  start <- Sys.time()

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'TargetCohorts.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,

    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,
    attrition_table = attritionTable,
    target_settings_schema = characterizationDatabaseSchema,
    target_settings_table = targetSettingsTable,

    limit_to_first_in_n_days = settings$limitToFirstInNDays,
    min_prior_observation = settings$minPriorObservation,
    cohort_ids = paste0(settings$targetIds, collapse = ','),

    cohort_schema = targetDatabaseSchema,
    cohort_table = targetTable,
    cdm_database_schema = cdmDatabaseSchema
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )
  completionTime <- Sys.time() - start

  if(incremental){
    readr::write_csv(
      file = file.path(executionPath,'cohort_job_tracker.csv'),
      x = data.frame(
        jobId = jobId,
        completeDate = date()
      ),
      append = TRUE
    )
  }

  message(paste0("Creating Targets: took ", round(completionTime, digits = 1), " ", units(completionTime)))

  return(invisible(TRUE))
}


generateCases <- function(
    connection,
    cdmDatabaseSchema,
    characterizationTable,
    attritionTable,
    targetSettingsTable,
    caseSettingsTable,
    characterizationDatabaseSchema,
    tempEmulationSchema,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    progressBar = interactive(),
    executionPath,
    settings,
    jobId,
    mode,
    incremental,
    casePreTargetDuration = 365,
    casePostOutcomeDuration = 365,
    ...
){

  message("Creating Cases")
  start <- Sys.time()

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'CaseCohorts.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,

    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,
    attrition_table = attritionTable,
    case_settings_schema = characterizationDatabaseSchema,
    case_settings_table = caseSettingsTable,

    outcome_cohort_ids = paste0(settings$outcomeIds, collapse = ','),
    characterization_target_ids = paste0(settings$characterizationTargetIds, collapse = ','),
    outcome_washout = settings$outcomeWashoutDays,
    risk_window_start = settings$riskWindowStart,
    start_anchor = settings$startAnchor,
    risk_window_end = settings$riskWindowEnd,
    end_anchor = settings$endAnchor,

    cohort_schema = outcomeDatabaseSchema,
    cohort_table = outcomeTable,

    include_risk_factor = settings$generateRiskFactors,
    include_case_series = settings$generateCaseSeries,
    case_series_before = casePreTargetDuration,
    case_series_after = casePostOutcomeDuration
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )
  completionTime <- Sys.time() - start

  if(incremental){
    readr::write_csv(
      file = file.path(executionPath,'cohort_job_tracker.csv'),
      x = data.frame(
        jobId = jobId,
        completeDate = date()
      ),
      append = TRUE
    )
  }
  message(paste0("Creating Cases: took ", round(completionTime, digits = 1), " ", units(completionTime)))

  return(invisible(TRUE))
}

generateNonCases <- function(
    connection,
    cdmDatabaseSchema,
    characterizationTable,
    attritionTable,
    targetSettingsTable,
    caseSettingsTable,
    characterizationDatabaseSchema,
    tempEmulationSchema,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    progressBar = interactive(),
    executionPath,
    settings,
    jobId,
    mode,
    incremental,
    ...
){

  message("Creating Non Cases")
  start <- Sys.time()

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'NonCaseCohorts.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,

    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,
    attrition_table = attritionTable,
    case_settings_schema = characterizationDatabaseSchema,
    case_settings_table = caseSettingsTable,

    outcome_cohort_ids = paste0(settings$outcomeIds, collapse = ','),
    characterization_target_ids = paste0(settings$characterizationTargetIds, collapse = ','),
    outcome_washout = settings$outcomeWashoutDays,
    risk_window_start = settings$riskWindowStart,
    start_anchor = settings$startAnchor,
    risk_window_end = settings$riskWindowEnd,
    end_anchor = settings$endAnchor,

    cohort_schema = outcomeDatabaseSchema,
    cohort_table = outcomeTable,

    use_plp = mode == 'PatientLevelPrediction',
    use_ci = mode == 'CohortIncidence'
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = progressBar,
    reportOverallTime = FALSE
  )
  completionTime <- Sys.time() - start

  if(incremental){
    readr::write_csv(
      file = file.path(executionPath,'cohort_job_tracker.csv'),
      x = data.frame(
        jobId = jobId,
        completeDate = date()
      ),
      append = TRUE
    )
  }
  message(paste0("Creating Non Cases: took ", round(completionTime, digits = 1), " ", units(completionTime)))

  return(invisible(TRUE))
}
