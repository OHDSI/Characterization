addDbAndSettings <- function(
    andromeda,
    databaseId,
    settingId
){

  # add databaseId and settingsId
  for(tableName in names(andromeda)){

    nrow <- andromeda[[tableName]] %>% dplyr::count() %>% dplyr::pull()
    # add database and setting ids
    if(nrow > 0){
      andromeda[[tableName]] <- andromeda[[tableName]] %>% dplyr::mutate(
        databaseId = !!databaseId,
        settingId = !!settingId
      )

    } else{
      andromeda[[tableName]] <- andromeda[[tableName]] %>% dplyr::mutate(
        databaseId = NA,
        settingId = NA
      )
    }
  }

  return(andromeda)

}


# this function saves the resulting Andromeda files
saveCharacterizationAndromeda <- function(
    andromeda,
    outputFolder
){

  saveLocation <- outputFolder
  if (!dir.exists(saveLocation)) {
    dir.create(saveLocation, recursive = TRUE)
  }

  # then save the Andromeda object
  Andromeda::saveAndromeda(
    andromeda = andromeda,
    fileName = file.path(saveLocation, 'result'),
    maintainConnection = FALSE,
    overwrite = TRUE
    )
}


checkExport <- function(outputDirectory){
  return(file.exists(file.path(outputDirectory,'export-complete.txt')))
}

confirmExport <- function(outputDirectory){
  utils::write.table(x = '', file = file.path(outputDirectory,'export-complete.txt'))
}

# this function exports Andromeda tables to csv results
exportAndromedaSubfilesToCsv <- function(
    executionPath,
    outputFolder,
    csvFilePrefix = 'c_',
    batchSize = 100000,
    minCellCount = 0,
    tablesToExport = c("analysisRef", "covariateRef",
                       "targetCovariatesContinuous", "targetCovariates",
                       "riskFactorCovariatesContinuous", "riskFactorCovariates",
                       "caseSeriesCovariatesContinuous", "caseSeriesCovariates",
                       "timeToEvent",
                       "rechallengeFailCaseSeries", "dechallengeRechallenge")
){

  if(!dir.exists(outputFolder)){
    message('Creating outputFolder directory')
    dir.create(outputFolder, recursive = TRUE)
  } else{
    message('Removing any existing results in outputFolder directory')
    tableName <- SqlRender::camelCaseToSnakeCase(tablesToExport)
    for(tbl in tableName){
      if(file.exists(file.path(outputFolder, paste0(csvFilePrefix,tbl, '.csv')))){
        message(paste0('Removing old ', paste0(csvFilePrefix,tbl, '.csv')))
        file.remove(file.path(outputFolder, paste0(csvFilePrefix,tbl, '.csv')))
      }
    }
  }

  csvTrackerFile <- file.path(outputFolder,'tracker.rds')
  tracker <- list(
    analysisRefTracker = c(),
    covariateRefTracker = c()
  )
  saveRDS(tracker, csvTrackerFile)

  folderNames <- dir(executionPath)
  isDir <- dir.exists(file.path(executionPath, folderNames))
  folderNames <- folderNames[isDir]

  # for each folder load covariates, covariates_continuous,
  # covariate_ref and analysis_ref
  for (folderName in folderNames) {

    if(file.exists(file.path(executionPath, folderName, 'result'))){
      message(paste0('Loading andromeda result at ', file.path(executionPath, folderName)))
      andromeda <- Andromeda::loadAndromeda(
        fileName = file.path(executionPath, folderName, 'result')
      )

      for (table in tablesToExport) {

        # export to snake case
        tableToExport <- SqlRender::camelCaseToSnakeCase(table)

        if(!is.null(andromeda[[table]])){
          # export the table to a csv file

          # remove redundant cov_ref and analysis_ref
          andromeda[[table]] <- removeRedundant(
            andromeda = andromeda,
            tableName = table,
            csvTrackerFile = csvTrackerFile
          )

          # get row count
          rowCount <- andromeda[[table]] %>% dplyr::count() %>% dplyr::pull()

          if(rowCount > 0){
            Andromeda::batchApply(
              tbl = andromeda[[table]],
              fun = function(data) {

                # 1) PROCESSING
                # censor results
                data <- censorResults(
                  data = data,
                  tableName = table,
                  minCellCount = minCellCount
                  )

                # convert the column names to snakecase
                colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

                # 2) Check file exists
                readr::write_csv(
                  x = formatDouble(data),
                  file = file.path(outputFolder, paste0(csvFilePrefix, tableToExport,".csv")),
                  append = file.exists(file.path(outputFolder, paste0(csvFilePrefix, tableToExport,".csv")))
                )

                return(TRUE)
              },
              batchSize = batchSize
            )
          } else{
            data <- as.data.frame(andromeda[[table]])
            if(!is.null(colnames(data))){
              # convert the column names to snakecase
              colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

              readr::write_csv(
                x = data,
                file = file.path(outputFolder, paste0(csvFilePrefix, tableToExport,".csv")),
                append = file.exists(file.path(outputFolder, paste0(csvFilePrefix, tableToExport,".csv")))
              )
            }
          }

        }
      }

    }

  }

  # add empty csv files for results that are not generated
  addMissingCsvs(
    outputFolder = outputFolder,
    tablesToExport = tablesToExport,
    csvFilePrefix = csvFilePrefix
    )

  # adding txt file that confirms export completed
  confirmExport(outputFolder)

return(invisible(TRUE))
}


addMissingCsvs <- function(
  outputFolder,
  tablesToExport,
  csvFilePrefix
){

  requiredTables <- SqlRender::camelCaseToSnakeCase(tablesToExport)
  presentTables <- gsub('.csv', '', gsub(csvFilePrefix, '', dir(outputFolder)))

  if(sum(!requiredTables %in% presentTables) > 0){
    missingTables <- requiredTables[!requiredTables %in% presentTables]
    tableDetails <- utils::read.csv(system.file('settings/resultsDataModelSpecification.csv',package = 'Characterization'))

    for(missingTable in missingTables){
      columnNames <- tableDetails$column_name[tableDetails$table_name == missingTable]

      df <- t(data.frame(rep(1, length(columnNames))))
      colnames(df) <- columnNames

      utils::write.csv(
        x = df[-1,],
        file = file.path(outputFolder, paste0(csvFilePrefix,missingTable , '.csv')),
        row.names = FALSE
        )

    }
  }

  return(invisible(TRUE))
}


removeRedundant <- function(
    andromeda,
    tableName,
    csvTrackerFile
){

  tracker <- readRDS(csvTrackerFile)

  if (tableName == "analysisRef") {
    andromeda[[tableName]] <- as.data.frame(andromeda[[tableName]]) %>%
      dplyr::mutate(
        uniqueId = paste0(.data$settingId, "-", as.character(format(as.double(.data$analysisId), nsmall = 0, scientific = FALSE, trim = TRUE )))
      ) %>%
      dplyr::filter( # need to filter analysis_id and setting_id
        !.data$uniqueId %in% tracker$analysisRefTracker
      )

    uniqueId <- andromeda[[tableName]] %>% dplyr::select("uniqueId") %>% dplyr::pull()

    andromeda[[tableName]] <- andromeda[[tableName]] %>%
      dplyr::select(-"uniqueId")

    tracker$analysisRefTracker <- unique(c(tracker$analysisRefTracker, uniqueId))
  }
  if (tableName == "covariateRef") { # this could be problematic as may have differnet covariate_ids
    andromeda[[tableName]] <- as.data.frame(andromeda[[tableName]]) %>%
      dplyr::mutate(
        uniqueId = paste0(.data$settingId, "-", as.character(format(as.double(.data$covariateId), nsmall = 0, scientific = FALSE, trim = TRUE )))
      ) %>%
      dplyr::filter( # need to filter covariate_id and setting_id
        !.data$uniqueId %in% tracker$covariateRefTracker
      )

    uniqueId <- andromeda[[tableName]] %>% dplyr::select("uniqueId") %>% dplyr::pull()

    andromeda[[tableName]] <- andromeda[[tableName]] %>%
      dplyr::select(-"uniqueId")

    tracker$covariateRefTracker <- unique(c(tracker$covariateRefTracker, uniqueId))
  }

  # save the updated tracker
  saveRDS(object = tracker, file = csvTrackerFile)

  # return the filtered object
  return(andromeda[[tableName]])
}

censorResults <- function(
    data,
    tableName,
    minCellCount = 0
){

  newData <- data

  # return empty data.frame if it is empty
  if(nrow(newData) == 0){
    return(newData)
  }

  if(tableName == 'targetCovariates'){
    # censor minCellCount columns sum_value
    removeInd <- newData$sumValue < minCellCount
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing sumValue counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        newData$sumValue[removeInd] <- -1 * minCellCount
        # adding other calculated columns
        newData$averageValue[removeInd] <- NA
      }
    }
  } else if(tableName == 'targetCovariatesContinuous'){
    removeInd <- newData$countValue < minCellCount
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing countValue counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        newData$countValue[removeInd] <- -1 * minCellCount
        # adding columns calculated from count
        newData$minValue[removeInd] <- NA
        newData$maxValue[removeInd] <- NA
        newData$averageValue[removeInd] <- NA
        newData$standardDeviation[removeInd] <- NA
        newData$medianValue[removeInd] <- NA
        newData$p10Value[removeInd] <- NA
        newData$p25Value[removeInd] <- NA
        newData$p75Value[removeInd] <- NA
        newData$p90Value[removeInd] <- NA
      }
    }
  } else if(tableName == 'riskFactorCovariates'){
    # censor minCellCount columns sum_value
    removeInd <- newData$caseSumValue < minCellCount & newData$caseSumValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing caseSumValue counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        newData$caseSumValue[removeInd] <- -1 * minCellCount
        # adding other calculated columns
        newData$caseAverageValue[removeInd] <- NA
      }
    }
    removeInd <- newData$nonCaseSumValue < minCellCount & newData$nonCaseSumValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing nonCaseSumValue counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        newData$nonCaseSumValue[removeInd] <- -1 * minCellCount
        # adding other calculated columns
        newData$nonCaseAverageValue[removeInd] <- NA
      }
    }
  } else if(tableName == 'riskFactorCovariatesContinuous'){
    removeInd <- newData$caseCountValue < minCellCount & newData$caseCountValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing caseCountValue counts less than ", minCellCount))
      if (sum(removeInd) > 0) {
        newData$caseCountValue[removeInd] <- -1 * minCellCount
        # adding columns calculated from count
        newData$caseMinValue[removeInd] <- NA
        newData$caseMaxValue[removeInd] <- NA
        newData$caseAverageValue[removeInd] <- NA
        newData$caseStandardDeviation[removeInd] <- NA
        newData$caseMedianValue[removeInd] <- NA
        newData$caseP10Value[removeInd] <- NA
        newData$caseP25Value[removeInd] <- NA
        newData$caseP75Value[removeInd] <- NA
        newData$caseP90Value[removeInd] <- NA
      }
    }
    removeInd <- newData$nonCaseCountValue < minCellCount & newData$nonCaseCountValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing nonCaseCountValue counts less than ", minCellCount))
        newData$nonCaseCountValue[removeInd] <- -1 * minCellCount
        # adding columns calculated from count
        newData$nonCaseMinValue[removeInd] <- NA
        newData$nonCaseMaxValue[removeInd] <- NA
        newData$nonCaseAverageValue[removeInd] <- NA
        newData$nonCaseStandardDeviation[removeInd] <- NA
        newData$nonCaseMedianValue[removeInd] <- NA
        newData$nonCaseP10Value[removeInd] <- NA
        newData$nonCaseP25Value[removeInd] <- NA
        newData$nonCaseP75Value[removeInd] <- NA
        newData$nonCaseP90Value[removeInd] <- NA
    }
  } else if(tableName == 'timeToEvent'){
    # TIME TO EVENT
    removeInd <- newData$numEvents < minCellCount & newData$numEvents != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing numEvents less than ", minCellCount))
      newData$numEvents[removeInd] <- -minCellCount
    }
  } else if(tableName == 'dechallengeRechallenge'){
    # DECHALLENDGE RECHALLENGE
    removeInd <- newData$numExposureEras < minCellCount & newData$numExposureEras != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring numExposureEras counts less than ", minCellCount))
      newData$numExposureEras[removeInd] <- -minCellCount
    }

    removeInd <- newData$numPersonsExposed < minCellCount & newData$numPersonsExposed != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring numPersonsExposed counts less than ", minCellCount))
      newData$numPersonsExposed [removeInd] <- -minCellCount
    }

    removeInd <- newData$numCases < minCellCount & newData$numCases != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring numCases counts less than ", minCellCount))
      newData$numCases[removeInd] <- -minCellCount
    }

    removeInd <- newData$dechallengeAttempt < minCellCount & newData$dechallengeAttempt != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring/removing dechallengeAttempt counts less than ", minCellCount))
      newData$dechallengeAttempt[removeInd] <- -minCellCount
      newData$pctDechallengeAttempt[removeInd] <- NA
    }

    removeInd <- (newData$dechallengeFail < minCellCount & newData$dechallengeFail !=0) | (newData$dechallengeSuccess < minCellCount & newData$dechallengeSuccess != 0)
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring/removing DECHALLENGE FAIL or SUCCESS counts less than ", minCellCount))
      newData$dechallengeFail[removeInd] <- -minCellCount
      newData$dechallengeSuccess[removeInd] <- -minCellCount
      newData$pctDechallengeFail[removeInd] <- NA
      newData$pctDechallengeSuccess[removeInd] <- NA
    }

    removeInd <- newData$rechallengeAttempt < minCellCount & newData$rechallengeAttempt != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring/removing rechallenge_attempt counts less than ", minCellCount))
      newData$rechallengeAttempt[removeInd] <- -minCellCount
      newData$pctRechallengeAttempt[removeInd] <- NA
    }

    removeInd <- (newData$rechallengeFail < minCellCount & newData$rechallengeFail != 0) | (newData$rechallengeSuccess < minCellCount & newData$rechallengeSuccess != 0)
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Censoring/removing rechallenge_fail or rechallenge_success counts less than ", minCellCount))
      newData$rechallengeFail[removeInd] <- -minCellCount
      newData$rechallengeSuccess[removeInd] <- -minCellCount
      newData$pctRechallengeFail[removeInd] <- NA
      newData$pctRechallengeSuccess[removeInd] <- NA
    }
  } else if(tableName == 'caseSeriesCovariates'){
    removeInd <- newData$beforeSumValue < minCellCount & newData$beforeSumValue != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing beforeSumValue counts less than ", minCellCount))
      newData$beforeSumValue[removeInd] <- -1 * minCellCount
        # adding other calculated columns
      newData$beforeAverageValue[removeInd] <- NA
    }

    removeInd <- newData$duringSumValue < minCellCount & newData$duringSumValue != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing duringSumValue counts less than ", minCellCount))
      newData$duringSumValue[removeInd] <- -1 * minCellCount
      # adding other calculated columns
      newData$duringAverageValue[removeInd] <- NA
    }

    removeInd <- newData$afterSumValue < minCellCount & newData$afterSumValue != 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing afterSumValue counts less than ", minCellCount))
      newData$afterSumValue[removeInd] <- -1 * minCellCount
      # adding other calculated columns
      newData$afterAverageValue[removeInd] <- NA
    }
  } else if(tableName == "caseSeriesCovariatesContinuous"){
    # TODO add cencoring for case series
    removeInd <- newData$beforeCountValue < minCellCount & newData$beforeCountValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing beforeCountValue counts less than ", minCellCount))
      newData$beforeCountValue[removeInd] <- -1 * minCellCount
      # adding columns calculated from count
      newData$beforeMinValue[removeInd] <- NA
      newData$beforeMaxValue[removeInd] <- NA
      newData$beforeAverageValue[removeInd] <- NA
      newData$beforeStandardDeviation[removeInd] <- NA
      newData$beforeMedianValue[removeInd] <- NA
      newData$beforeP10Value[removeInd] <- NA
      newData$beforeP25Value[removeInd] <- NA
      newData$beforeP75Value[removeInd] <- NA
      newData$beforeP90Value[removeInd] <- NA
    }

    removeInd <- newData$duringCountValue < minCellCount & newData$duringCountValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing duringCountValue counts less than ", minCellCount))
      newData$duringCountValue[removeInd] <- -1 * minCellCount
      # adding columns calculated from count
      newData$duringMinValue[removeInd] <- NA
      newData$duringMaxValue[removeInd] <- NA
      newData$duringAverageValue[removeInd] <- NA
      newData$duringStandardDeviation[removeInd] <- NA
      newData$duringMedianValue[removeInd] <- NA
      newData$duringP10Value[removeInd] <- NA
      newData$duringP25Value[removeInd] <- NA
      newData$duringP75Value[removeInd] <- NA
      newData$duringP90Value[removeInd] <- NA
    }


    removeInd <- newData$afterCountValue < minCellCount & newData$afterCountValue > 0
    if (sum(removeInd) > 0) {
      ParallelLogger::logInfo(paste0("Removing afterCountValue counts less than ", minCellCount))
      newData$afterCountValue[removeInd] <- -1 * minCellCount
      # adding columns calculated from count
      newData$afterMinValue[removeInd] <- NA
      newData$afterMaxValue[removeInd] <- NA
      newData$afterAverageValue[removeInd] <- NA
      newData$afterStandardDeviation[removeInd] <- NA
      newData$afterMedianValue[removeInd] <- NA
      newData$afterP10Value[removeInd] <- NA
      newData$afterP25Value[removeInd] <- NA
      newData$afterP75Value[removeInd] <- NA
      newData$afterP90Value[removeInd] <- NA
    }

  }

  return(newData)
}


exportAttrition <- function(
    executionPath,
    outputFolder,
    csvFilePrefix = 'c_',
    minCellCount = 0
){

  # export target attrition
  if(file.exists(file.path(executionPath, 'target_attrition', 'result'))){
    andromeda <- Andromeda::loadAndromeda(file.path(executionPath, 'target_attrition', 'result'))

    # censor
    data <- andromeda$target_attrition %>% dplyr::mutate(
      nEvents = ifelse(.data$nEvents < !!minCellCount & .data$nEvents > 0, -1*minCellCount, .data$nEvents),
      nPeople = ifelse(.data$nPeople < !!minCellCount & .data$nPeople > 0, -1*minCellCount, .data$nPeople)
    ) %>%
      dplyr::collect()

    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

    # save the attrition
    utils::write.csv(
      x = data,
      file = file.path(outputFolder, paste0(csvFilePrefix, 'target_attrition', '.csv')),
      row.names = FALSE
        )
  }

  # export case attrition
  if(file.exists(file.path(executionPath, 'case_attrition', 'result'))){
    andromeda <- Andromeda::loadAndromeda(file.path(executionPath, 'case_attrition', 'result'))

    # censor
    data <- andromeda$case_attrition %>% dplyr::mutate(
      nEvents = ifelse(.data$nEvents < !!minCellCount & .data$nEvents > 0, -1*minCellCount, .data$nEvents),
      nPeople = ifelse(.data$nPeople < !!minCellCount & .data$nPeople > 0, -1*minCellCount, .data$nPeople)
    ) %>%
      dplyr::collect()

    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

    # save the attrition
    utils::write.csv(
      x = data,
      file = file.path(outputFolder, paste0(csvFilePrefix, 'case_attrition', '.csv')),
      row.names = FALSE
    )
  }

  return(invisible(TRUE))
}



exportCounts <- function(
    executionPath,
    outputFolder,
    csvFilePrefix = 'c_',
    minCellCount = 0
){

  # export target attrition
  if(file.exists(file.path(executionPath, 'target_counts', 'result'))){
    andromeda <- Andromeda::loadAndromeda(file.path(executionPath, 'target_counts', 'result'))

    # censor
    data <- andromeda$target_counts %>% dplyr::mutate(
      nEvents = ifelse(.data$nEvents < !!minCellCount & .data$nEvents > 0, -1*minCellCount, .data$nEvents),
      nPeople = ifelse(.data$nPeople < !!minCellCount & .data$nPeople > 0, -1*minCellCount, .data$nPeople)
    ) %>%
      dplyr::collect()

    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

    # save the attrition
    utils::write.csv(
      x = data,
      file = file.path(outputFolder, paste0(csvFilePrefix, 'target_counts', '.csv')),
      row.names = FALSE
    )
  }

  # export case attrition
  if(file.exists(file.path(executionPath, 'case_counts', 'result'))){
    andromeda <- Andromeda::loadAndromeda(file.path(executionPath, 'case_counts', 'result'))

    # censor if the cases or non-cases are < min count
    remove <- andromeda$case_counts %>% dplyr::filter(
      .data$nEvents < !!minCellCount & .data$nEvents > 0,
      .data$nPeople < !!minCellCount & .data$nPeople > 0
    ) %>%
      dplyr::select("characterizationCaseId") %>%
      dplyr::distinct() %>%
      dplyr::mutate(
        filter = TRUE
      ) %>%
      dplyr::collect()

    data <- andromeda$case_counts %>%
      dplyr::left_join(
        y = remove,
        by = "characterizationCaseId",
        copy = TRUE
        ) %>%
      dplyr::mutate(filter = dplyr::if_else(is.na(.data$filter), FALSE,.data$filter)) %>%
      dplyr::mutate(
        nEvents = ifelse(.data$filter, NA, .data$nEvents),
        nPeople = ifelse(.data$filter, NA, .data$nPeople)
      ) %>%
      dplyr::select(-"filter") %>%
      dplyr::collect()

    colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))

    # save the attrition
    utils::write.csv(
      x = data,
      file = file.path(outputFolder, paste0(csvFilePrefix, 'case_counts', '.csv')),
      row.names = FALSE
    )
  }

  return(invisible(TRUE))
}



