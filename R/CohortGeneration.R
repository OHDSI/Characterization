generateCohorts <- function(
    characterizationSettings,
    threads = 1,
    nTargetJobs = 1,
    mode,
    incremental,
    executionPath = executionPath,
    connectionDetails,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    nestingCohortDatabaseSchema = targetDatabaseSchema,
    nestingCohortTable = targetTable,
    outputDatabaseSchema = targetDatabaseSchema,
    outputTable = 'characterization_cohort',
    cdmDatabaseSchema,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    progressBar = interactive(),
    settingHash,
    dbHash
){

  # connection
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # tables names
  characterizationTableWithHash <- paste0(outputTable, '_',settingHash, '_', dbHash)
  outcomeEraTableWithHash <- paste0('outcome_era', '_',settingHash, '_', dbHash)

  targetSettingsTableWithHash <- paste0('target_settings', '_',settingHash, '_', dbHash)
  targetAttritionTableWithHash <- paste0('target_attrition', '_',settingHash, '_', dbHash)
  targetCountTableWithHash <- paste0('target_count', '_',settingHash, '_', dbHash)

  caseSettingsTableWithHash <- paste0('case_settings', '_',settingHash, '_', dbHash)
  caseAttritionTableWithHash <- paste0('case_attrition', '_',settingHash, '_', dbHash)
  caseCountTableWithHash <- paste0('case_count', '_',settingHash, '_', dbHash)


  cohortJobs <- getCohortJobs(
    characterizationSettings,
    mode = mode,
    nTargetJobs = nTargetJobs
  )

  # only run the code below if there are cohorts to be generated
  if(!is.null(cohortJobs$jobs)){

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
      dropTableIfExists = TRUE, # changed from FALSE,
      createTable = TRUE, # changed from FALSE,
      data = cohortJobs$targets,
      camelCaseToSnakeCase = TRUE,
      progressBar = progressBar
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
      camelCaseToSnakeCase = TRUE,
      progressBar = progressBar
    )
  }

  if(incremental){

    if(!file.exists(file.path(executionPath,'cohort_job_tracker.csv'))){
      sql <- SqlRender::loadRenderTranslateSql(
        sqlFilename = 'CreateTargetCohortTable.sql',
        packageName = 'Characterization',
        dbms = attributes(connection)$dbms,
        tempEmulationSchema = tempEmulationSchema,
        characterization_schema = outputDatabaseSchema,
        characterization_table = characterizationTableWithHash,
        target_attrition_table = targetAttritionTableWithHash,
        target_count_table = targetCountTableWithHash,
        case_attrition_table = caseAttritionTableWithHash,
        case_count_table = caseCountTableWithHash,
        outcome_era_table = outcomeEraTableWithHash
      )

      DatabaseConnector::executeSql(connection, sql, progressBar = progressBar)

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

    } else{ # replace with readr read?
      tracker <- utils::read.csv(file.path(executionPath,'cohort_job_tracker.csv'))
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
      dbms = attributes(connection)$dbms,
      tempEmulationSchema = tempEmulationSchema,
      characterization_schema = outputDatabaseSchema,
      characterization_table = characterizationTableWithHash,
      target_attrition_table = targetAttritionTableWithHash,
      target_count_table = targetCountTableWithHash,
      case_attrition_table = caseAttritionTableWithHash,
      case_count_table = caseCountTableWithHash,
      outcome_era_table = outcomeEraTableWithHash
      )

    DatabaseConnector::executeSql(connection, sql, progressBar = progressBar)

  }

    if(nrow(cohortJobs$jobs) > 0){
      # TODO: replace below with parallel jobs using threads
      message("Creating new cluster")
      cohortcluster <- ParallelLogger::makeCluster(
        numberOfThreads = threads,
        singleThreadToMain = TRUE,
        setAndromedaTempFolder = TRUE
      )
      on.exit(ParallelLogger::stopCluster(cluster = cohortcluster))

      ParallelLogger::clusterApply(
        cluster = cohortcluster,
        x = lapply(
          1:nrow(cohortJobs$jobs),
          function(i){
            list(
              func = cohortJobs$jobs[i,"functionName"],
              connectionDetails = connectionDetails,
              cdmDatabaseSchema = cdmDatabaseSchema,
              characterizationTable = characterizationTableWithHash,
              targetAttritionTable = targetAttritionTableWithHash,
              caseAttritionTable = caseAttritionTableWithHash,
              targetCountTable = targetCountTableWithHash,
              caseCountTable = caseCountTableWithHash,
              targetSettingsTable = targetSettingsTableWithHash,
              caseSettingsTable = caseSettingsTableWithHash,
              characterizationDatabaseSchema = outputDatabaseSchema,
              tempEmulationSchema = tempEmulationSchema,
              targetDatabaseSchema = targetDatabaseSchema,
              targetTable = targetTable,
              outcomeDatabaseSchema = outcomeDatabaseSchema,
              outcomeTable = outcomeTable,
              nestingCohortDatabaseSchema = nestingCohortDatabaseSchema,
              nestingCohortTable = nestingCohortTable,
              incremental = incremental,
              mode = mode,

              casePreTargetDuration = casePreTargetDuration,
              casePostOutcomeDuration = casePostOutcomeDuration,

              progressBar = progressBar, # set to FALSE
              executionPath = executionPath,

              settings = ParallelLogger::convertJsonToSettings(cohortJobs$jobs[i,"settings"]),
              jobId = cohortJobs$jobs[i, "jobId"],
              
              outcomeEraTable = outcomeEraTableWithHash

            )

          }
        ),
        fun = runCohortGenerationInParallel,
        progressBar = progressBar,
        stopOnError = TRUE # do not proceed unless all cohort jobs complete
      )

    } else{ # end if no jobs left
      message('No cohort jobs left to run')
    }
  } # end not null joblist


return(list(
  characterizationTable = characterizationTableWithHash,
  targetSettingsTable = targetSettingsTableWithHash,
  caseSettingsTable = caseSettingsTableWithHash,
  targetAttritionTable = targetAttritionTableWithHash,
  caseAttritionTable = caseAttritionTableWithHash,
  targetCountTable = targetCountTableWithHash,
  caseCountTable = caseCountTableWithHash,
  outcomeEraTable = outcomeEraTableWithHash
)
)
}



runCohortGenerationInParallel <- function(x){

  functionToCall <- paste0('Characterization:::', x$func, sep= '')
  x$func <- NULL

  do.call(
    what = eval(parse(text = functionToCall)),
    args = x
  )
}



# make this return data.frame with columns functionName/settings/executionFolder/jobId
# where settings into functionName is a json: as.character(ParallelLogger::convertSettingsToJson(list))
# partitions by targetId (thread) and settings
getCohortJobs <- function(
    characterizationSettings,
    mode,
    nTargetJobs
){

  message('Extracting cohort jobs')
  targets <- characterizationSettings$characterizationTargetLookup
  cases <- c()

  # Extracting Risk Factor targets and cases
  if(!is.null(characterizationSettings$riskFactorSettings)){

    tempCases <- do.call(
      what = 'rbind',
      args = lapply(
        X = characterizationSettings$riskFactorSettings,
        FUN = function(x){
          do.call(
            what = 'rbind',
            lapply(
              X = unique(x$characterizationTargetIds),
              FUN = function(y){
                data.frame(
                  characterizationTargetId = y,
                  outcomeId = x$outcomeIds,
                  outcomeWashoutDays = x$outcomeWashoutDays,
                  riskWindowStart = x$riskWindowStart,
                  startAnchor = x$startAnchor,
                  riskWindowEnd = x$riskWindowEnd,
                  endAnchor = x$endAnchor,
                  riskFactorSettings = 1,
                  caseSeriesSettings = 0
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

    tempCases <- do.call(
      what = 'rbind',
      args = lapply(
        X = characterizationSettings$caseSeriesSettings,
        FUN = function(x){
          do.call(
            what = 'rbind',
            lapply(
              X = unique(x$characterizationTargetIds),
              FUN = function(y){
                data.frame(
                  characterizationTargetId = y,
                  outcomeId = x$outcomeIds,
                  outcomeWashoutDays = x$outcomeWashoutDays,
                  riskWindowStart = x$riskWindowStart,
                  startAnchor = x$startAnchor,
                  riskWindowEnd = x$riskWindowEnd,
                  endAnchor = x$endAnchor,
                  riskFactorSettings = 0,
                  caseSeriesSettings = 1
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

    jobCols <- c("targetId")
    settingsCols <- c("limitToFirstInNDays", "minPriorObservation",
                      "nestingCohortId", "minAge", "maxAge",
                      "studyStart", "studyEnd", "genderConceptIds")

    jobSettings <- targets %>%
      dplyr::ungroup() %>%
      dplyr::select(dplyr::all_of(jobCols)) %>%
      dplyr::distinct()

    jobSettings$nTargetJobs <- rep(1:nTargetJobs, ceiling(nrow(jobSettings) / nTargetJobs))[1:nrow(jobSettings)]
    targets <- merge(targets, jobSettings, by = jobCols)

    targets <- unique(targets) %>%
      dplyr::inner_join(
        y = targets %>%
          dplyr::select(dplyr::all_of(settingsCols)) %>%
          dplyr::distinct() %>%
          dplyr::arrange(dplyr::pick(dplyr::all_of(settingsCols))) %>%
          dplyr::mutate(
            settingId = dplyr::row_number()
          ),
        by = settingsCols
      )


    message(paste0('Adding ', length(unique(targets$settingId))*length(unique(targets$nTargetJobs)) ,' Target Cohort Jobs containing ', length(unique(targets$targetId)) ,' targets'))

    for(setId in unique(targets$settingId)){
      toi <- targets %>%
        dplyr::filter(.data$settingId == !!setId)

      settingVal <- toi[1,settingsCols]

      for (i in unique(toi$nTargetJobs)) {
        ind <- toi$nTargetJobs== i

        jobs <- rbind(jobs,data.frame(
          functionName = 'generateTargets',
          settings =  as.character(ParallelLogger::convertSettingsToJson(
            list(
              settingId = setId,
              targetIds = unique(toi$targetId[ind]),
              limitToFirstInNDays = unique(toi$limitToFirstInNDays[ind]),
              minPriorObservation = unique(toi$minPriorObservation[ind]),
              nestingCohortId = unique(toi$nestingCohortId[ind]),
              minAge = unique(toi$minAge[ind]),
              maxAge = unique(toi$maxAge[ind]),
              studyStart = unique(toi$studyStart[ind]),
              studyEnd = unique(toi$studyEnd[ind]),
              genderConceptIds = unique(toi$genderConceptIds[ind])
            ))),
          jobId = paste("targets",i, paste0(settingVal, collapse = "_"), sep = "_")
        ))
      }

    }
  }


  # add in job for outcome eras per washout
  # only run if there are cases and mode is not Efficient
  # since efficient mode doesnt need the outcomes
  # THIS NEEDS TO BE RUN BEFOR NON-CASE generation
  if(!is.null(nrow(cases))){
    if(mode != 'Efficient'){
      ooi <- unique(cases[, c('outcomeId', 'outcomeWashoutDays')])
      for(outcomeWashoutDay in unique(ooi$outcomeWashoutDays)){
        jobs <- rbind(jobs, data.frame(
          functionName = 'generateOutcomeEras',
          settings = as.character(ParallelLogger::convertSettingsToJson(list(
            outcomeIds = ooi$outcomeId[ooi$outcomeWashoutDays == outcomeWashoutDay],
            outcomeWashoutDays = outcomeWashoutDay
          )
          )),
          jobId = paste("outcome_eras",i,outcomeWashoutDay, sep = "_")
        ))
      }
    }
  }


  if(!is.null(nrow(cases))){

    cases <- unique(cases) %>%
      dplyr::group_by(
        .data$characterizationTargetId,
        .data$outcomeWashoutDays, .data$outcomeId,
        .data$riskWindowStart,.data$startAnchor,
        .data$riskWindowEnd, .data$endAnchor
      ) %>%
      dplyr::summarize(
        riskFactorSettings = max(.data$riskFactorSettings),
        caseSeriesSettings = max(.data$caseSeriesSettings)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::inner_join(
        y = cases %>%
          dplyr::distinct(.data$outcomeWashoutDays, #.data$outcomeId,
                          .data$riskWindowStart,.data$startAnchor,
                          .data$riskWindowEnd, .data$endAnchor) %>%
          dplyr::arrange(.data$outcomeWashoutDays, #.data$outcomeId,
                         .data$riskWindowStart,.data$startAnchor,
                         .data$riskWindowEnd, .data$endAnchor) %>%
          dplyr::mutate(
            settingId = dplyr::row_number()
          ),
        by = c("outcomeWashoutDays", #"outcomeId",
               "riskWindowStart", "startAnchor",
               "riskWindowEnd", "endAnchor")
      ) %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        characterizationCaseId = dplyr::row_number()
      )

    # add nTargetJobs using characterizationTargetId
    jobCols <- c("characterizationTargetId")
    jobSettings <- cases %>%
      dplyr::ungroup() %>%
      dplyr::select(dplyr::all_of(jobCols)) %>%
      dplyr::distinct()

    jobSettings$nTargetJobs <- rep(1:nTargetJobs, ceiling(nrow(jobSettings) / nTargetJobs))[1:nrow(jobSettings)]
    cases <- merge(cases, jobSettings, by = jobCols)

    message(paste0('Adding ', length(unique(cases$settingId))*length(unique(cases$nTargetJobs)) ,' Case Cohort Jobs containing ', nrow(cases), ' case cohorts'))

    nNonCase <- 0
    for(setId in unique(cases$settingId)){
      coi <- cases %>%
        dplyr::filter(.data$settingId == !!setId)

      settingVal <- coi[1,c("outcomeWashoutDays", #"outcomeId",
                            "riskWindowStart", "startAnchor",
                            "riskWindowEnd", "endAnchor")]

      for (i in unique(coi$nTargetJobs)) {
        ind <- coi$nTargetJobs== i

        jobs <- rbind(jobs, data.frame(
          functionName = 'generateCases',
          settings = as.character(ParallelLogger::convertSettingsToJson(list(
            settingId = setId,
            analysisId = 2,
            characterizationTargetIds = unique(coi$characterizationTargetId[ind]),
            outcomeIds = unique(coi$outcomeId[ind]),
            outcomeWashoutDays = unique(coi$outcomeWashoutDays[ind]),
            riskWindowStart = unique(coi$riskWindowStart[ind]),
            startAnchor = unique(coi$startAnchor[ind]),
            riskWindowEnd = unique(coi$riskWindowEnd[ind]),
            endAnchor = unique(coi$endAnchor[ind]),
            generateRiskFactors = max(coi$riskFactorSettings[ind]) ,
            generateCaseSeries = max(coi$caseSeriesSettings[ind])
          )
          )),
          jobId = paste("cases",i, paste0(settingVal, collapse = "_"), sep = "_")
        ))


        if(mode != 'Efficient'){

          if(max(coi$riskFactorSettings[ind]) == 1){
            nNonCase <- nNonCase + 1
            jobs <- rbind(jobs, data.frame(
              functionName = 'generateNonCases',
              settings = as.character(ParallelLogger::convertSettingsToJson(list(
                settingId = setId,
                characterizationTargetIds = unique(coi$characterizationTargetId[ind]),
                outcomeIds = unique(coi$outcomeId[ind]),
                outcomeWashoutDays = unique(coi$outcomeWashoutDays[ind]),
                riskWindowStart = unique(coi$riskWindowStart[ind]),
                startAnchor = unique(coi$startAnchor[ind]),
                riskWindowEnd = unique(coi$riskWindowEnd[ind]),
                endAnchor = unique(coi$endAnchor[ind])
              )
              )),
              jobId = paste("non_cases",i, paste0(settingVal, collapse = "_"), sep = "_")
            ))
          }

        } # not efficient
      }
    }
    message(paste0('Adding ',nNonCase,' Non-Case Cohort Jobs'))

  }


  # removing nTargetJobs
  if(!is.null(nrow(targets))){
    targets <- targets %>% dplyr::select(-"nTargetJobs")
  }
  if(!is.null(nrow(cases))){
    cases <- cases %>% dplyr::select(-"nTargetJobs")
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
    connectionDetails,
    cdmDatabaseSchema,
    characterizationTable,
    targetAttritionTable,
    targetCountTable,
    targetSettingsTable,
    characterizationDatabaseSchema,
    tempEmulationSchema,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema,
    outcomeTable,
    nestingCohortDatabaseSchema,
    nestingCohortTable,
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

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'TargetCohorts.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,

    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,
    target_attrition_table = targetAttritionTable,
    target_count_table = targetCountTable,
    target_settings_schema = characterizationDatabaseSchema,
    target_settings_table = targetSettingsTable,

    limit_to_first_in_n_days = settings$limitToFirstInNDays,
    min_prior_observation = settings$minPriorObservation,
    nesting_cohort_id = settings$nestingCohortId,
    min_age = settings$minAge,
    max_age = settings$maxAge,
    gender_concept_ids = settings$genderConceptIds,
    study_start = settings$studyStart,
    study_end = settings$studyEnd,

    cohort_ids = paste0(settings$targetIds, collapse = ','),

    cohort_schema = targetDatabaseSchema,
    cohort_table = targetTable,

    nesting_schema = nestingCohortDatabaseSchema,
    nesting_table = nestingCohortTable,

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
    connectionDetails,
    cdmDatabaseSchema,
    characterizationTable,
    caseAttritionTable,
    caseCountTable,
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

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'CaseCohorts.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,

    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,
    #case_attrition_table = caseAttritionTable,
    case_count_table = caseCountTable,
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
    connectionDetails,
    cdmDatabaseSchema,
    characterizationTable,
    outcomeEraTable,
    caseAttritionTable,
    caseCountTable,
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
    restrictWashoutToObs,
    ...
){

  message("Creating Non Cases")
  start <- Sys.time()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'NonCaseCohorts.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,

    characterization_schema = characterizationDatabaseSchema,
    characterization_table = characterizationTable,
    case_attrition_table = caseAttritionTable,
    case_count_table = caseCountTable,
    case_settings_schema = characterizationDatabaseSchema,
    case_settings_table = caseSettingsTable,

    outcome_cohort_ids = paste0(settings$outcomeIds, collapse = ','),
    characterization_target_ids = paste0(settings$characterizationTargetIds, collapse = ','),
    outcome_era_table = outcomeEraTable,
    outcome_washout = settings$outcomeWashoutDays,
    risk_window_start = settings$riskWindowStart,
    start_anchor = settings$startAnchor,
    risk_window_end = settings$riskWindowEnd,
    end_anchor = settings$endAnchor,

    cohort_schema = outcomeDatabaseSchema,
    cohort_table = outcomeTable,

    use_plp = mode == 'PatientLevelPrediction',
    use_ci = mode == 'CohortIncidence',

    restrict_washout_to_obs = restrictWashoutToObs
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

generateOutcomeEras <- function(
    connectionDetails,
    cdmDatabaseSchema,
    characterizationTable,
    caseAttritionTable,
    caseCountTable,
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
    outcomeEraTable,
    ...
){

  message(paste("Creating outcome eras for washout ", settings$outcomeWashoutDays))
  start <- Sys.time()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'OutcomeEras.sql',
    packageName = 'Characterization',
    dbms =  attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_schema = characterizationDatabaseSchema,
    outcome_era_table = outcomeEraTable,
    outcome_ids = paste0(settings$outcomeIds, collapse = ','),
    outcome_washout = settings$outcomeWashoutDays,
    cohort_schema = outcomeDatabaseSchema,
    cohort_table = outcomeTable
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
  message(paste0("Creating Outcome Eras: took ", round(completionTime, digits = 1), " ", units(completionTime)))

  return(invisible(TRUE))

}



dropCohorts <- function(
  connectionDetails,
  outputDatabaseSchema,
  outputTable = 'characterization_cohort',
  cdmDatabaseSchema,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  progressBar = FALSE,
  settingHash,
  dbHash
){
  # remove the tables if they exist

  # connection
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  # tables names
  characterizationTableWithHash <- paste0(outputTable, '_',settingHash, '_', dbHash)
  targetSettingsTableWithHash <- paste0('target_settings', '_',settingHash, '_', dbHash)
  caseSettingsTableWithHash <- paste0('case_settings', '_',settingHash, '_', dbHash)
  targetAttritionTableWithHash <- paste0('target_attrition', '_',settingHash, '_', dbHash)
  caseAttritionTableWithHash <- paste0('case_attrition', '_',settingHash, '_', dbHash)
  targetCountTableWithHash <- paste0('target_count', '_',settingHash, '_', dbHash)
  caseCountTableWithHash <- paste0('case_count', '_',settingHash, '_', dbHash)
  outcomeEraTableWithHash <- paste0('outcome_era', '_',settingHash, '_', dbHash)


  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = 'DropTargetCohortTable.sql',
    packageName = 'Characterization',
    dbms = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema,
    characterization_schema = outputDatabaseSchema,
    characterization_table = characterizationTableWithHash,
    target_attrition_table = targetAttritionTableWithHash,
    case_attrition_table = caseAttritionTableWithHash,
    target_count_table = targetCountTableWithHash,
    case_count_table = caseCountTableWithHash,
    target_settings_table = targetSettingsTableWithHash,
    case_settings_table = caseSettingsTableWithHash,
    outcome_era_table = outcomeEraTableWithHash
  )

  DatabaseConnector::executeSql(connection, sql, progressBar = progressBar)

  return(invisible(TRUE))
}
