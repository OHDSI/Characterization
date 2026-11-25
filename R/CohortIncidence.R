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

#' Create cohort incidence study settings
#'
#' @param studyPopulationSettings An object created using \code{createStudyPopulationSettings} or a list of \code{createStudyPopulationSettings} that specifies cohort inclusion criteria
#' @param outcomeIds   A vector of cohortIds for the outcome cohorts
#' @param outcomeWashoutDays A vector of integers specifying the washout days for the outcomeIds
#' @template timeAtRisks
#' @param byAge Whether to stratify the incidence rates by age groups (specified via ageBreaks or ageBreakList)
#' @param byGender Whether to stratify the incidence rates by gender
#' @param byYear Whether to stratify the incidence rates by index year
#' @param ageBreaks a vector of integers indicating the age group bounds
#' @param ageBreakList a list of ageBreaks, used to specify multiple age break strata.
#' @param startDate	 a character vector representing a date in YYYY-MM-DD format
#' @param endDate	 a character vector representing a date in YYYY-MM-DD format
#'
#'
#' @family CohortIncidence
#'
#' @return
#' An list with the cohort incidence settings
#'
#' @examples
#' # example code
#'
#' ciSet <- createCohortIncidenceSettings(
#'   studyPopulationSettings = createStudyPopulationSettings(
#'     targetIds = c(1,2),
#'     limitToFirstInNDays = 0,
#'     minPriorObservation = 0
#'     ),
#'   outcomeIds = 3
#' )
#'
#'
#' @export
createCohortIncidenceSettings <- function(
    studyPopulationSettings,
    outcomeIds,
    outcomeWashoutDays = 0,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    byAge = FALSE,
    byGender = FALSE,
    byYear = FALSE,
    ageBreaks = NULL,
    ageBreakList = NULL,
    startDate = '',
    endDate = ''
) {
  ensure_installed("CohortIncidence")

  errorMessages <- checkmate::makeAssertCollection()

  .checkCohortIds(
    cohortIds = outcomeIds,
    type = "outcome",
    errorMessages = errorMessages
  )

  checkmate::assertIntegerish(
    x = outcomeWashoutDays,
    lower = 0,
    any.missing = FALSE,
    add = errorMessages
  )
  checkmate::assertTRUE(
    x = length(outcomeWashoutDays) %in% c(1, length(outcomeIds)),
    add = errorMessages
  )

  # check the TARs are the same length
  checkmate::assert_true(length(riskWindowStart) == length(startAnchor), add = errorMessages)
  checkmate::assert_true(length(riskWindowStart) == length(riskWindowEnd), add = errorMessages)
  checkmate::assert_true(length(riskWindowStart) == length(endAnchor), add = errorMessages)

  for(i in 1:length(riskWindowStart)){
    .checkTimeAtRisk(
      riskWindowStart = riskWindowStart[i],
      startAnchor = startAnchor[i],
      riskWindowEnd = riskWindowEnd[i],
      endAnchor = endAnchor[i],
      errorMessages = errorMessages
    )
  }

  checkmate::assertFlag(x = byAge, add = errorMessages)
  checkmate::assertFlag(x = byGender, add = errorMessages)
  checkmate::assertFlag(x = byYear, add = errorMessages)
  checkmate::assertIntegerish(
    x = ageBreaks,
    null.ok = TRUE,
    any.missing = FALSE,
    add = errorMessages
  )
  checkmate::assertList(
    x = ageBreakList,
    null.ok = TRUE,
    add = errorMessages
  )
  if (!is.null(ageBreakList)) {
    lapply(ageBreakList, function(ageBreaks) {
      checkmate::assertIntegerish(
        x = ageBreaks,
        any.missing = FALSE,
        add = errorMessages
      )
    })
  }
  checkmate::assertTRUE(
    x = is.null(ageBreaks) || is.null(ageBreakList),
    add = errorMessages
  )
  if (byAge) {
    checkmate::assertTRUE(
      x = !is.null(ageBreaks) || !is.null(ageBreakList),
      add = errorMessages
    )
  }
  checkmate::assertCharacter(
    x = startDate,
    len = 1,
    pattern = "^$|^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    add = errorMessages
  )
  checkmate::assertCharacter(
    x = endDate,
    len = 1,
    pattern = "^$|^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    add = errorMessages
  )
  parsedStartDate <- as.Date(startDate, format = "%Y-%m-%d")
  parsedEndDate <- as.Date(endDate, format = "%Y-%m-%d")
  checkmate::assertTRUE(
    x = !nzchar(startDate) || !is.na(parsedStartDate),
    add = errorMessages
  )
  checkmate::assertTRUE(
    x = !nzchar(endDate) || !is.na(parsedEndDate),
    add = errorMessages
  )
  checkmate::assertTRUE(
    x = !nzchar(startDate) || !nzchar(endDate) || parsedStartDate <= parsedEndDate,
    add = errorMessages
  )
  checkmate::reportAssertions(errorMessages)


  # create data.frame with all combinations
  result <- list(
    studyPopulationSettings = combineStudyPopulationSettings(studyPopulationSettings),
    outcomeIds = outcomeIds,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    byAge = byAge,
    byGender = byGender,
    byYear = byYear,
    ageBreaks = ageBreaks,
    ageBreakList = ageBreakList,
    startDate = startDate,
    endDate = endDate
  )

  class(result) <- "cohortIncidenceSettings"
  return(result)
}

createCohortIncidenceDesign <- function(settings) {
  targetIds <- settings$characterizationTargetIds
  outcomeIds <- settings$outcomeIds
  outcomeWashoutDays <- settings$outcomeWashoutDays
  if (length(outcomeIds) > 1 && length(outcomeWashoutDays) == 1) {
    outcomeWashoutDays <- rep(outcomeWashoutDays, length(outcomeIds))
  }

  outcomeDefs <- lapply(seq_along(outcomeIds), function(index) {
    CohortIncidence::createOutcomeDef(
      id = index * 10 + 7,
      name = paste0("cohort ", outcomeIds[index]),
      cleanWindow = outcomeWashoutDays[index],
      cohortId = outcomeIds[index]
    )
  })
  anchorLookup <- c("cohort start" = "start", "cohort end" = "end")

  tar <- lapply(
    X = 1:length(settings$riskWindowStart),
    FUN = function(i){
      CohortIncidence::createTimeAtRiskDef(
        id = i,
        startOffset = settings$riskWindowStart[i],
        startWith = anchorLookup[[settings$startAnchor[i]]],
        endOffset = settings$riskWindowEnd[i],
        endWith = anchorLookup[[settings$endAnchor[i]]]
      )
    })

  strataArguments <- list(
    byAge = settings$byAge,
    byGender = settings$byGender,
    byYear = settings$byYear
  )
  if (!is.null(settings$ageBreaks)) {
    strataArguments$ageBreaks <- settings$ageBreaks
  }
  if (!is.null(settings$ageBreakList)) {
    strataArguments$ageBreakList <- settings$ageBreakList
  }
  designArguments <- list(
    analysisList = list(CohortIncidence::createIncidenceAnalysis(
      tars = 1:length(tar),
      outcomes = seq_along(outcomeIds) * 10 + 7,
      targets = targetIds
    )),
    tars = tar, # removed list
    outcomeDefs = outcomeDefs,
    targetDefs = lapply(targetIds, function(targetId) {
      CohortIncidence::createCohortRef(
        id = targetId,
        name = paste0("cohort ", targetId)
      )
    }),
    strataSettings = do.call(CohortIncidence::createStrataSettings, strataArguments)
  )
  if (nzchar(settings$startDate) || nzchar(settings$endDate)) {
    dateArguments <- list()
    if (nzchar(settings$startDate)) {
      dateArguments$startDate <- settings$startDate
    }
    if (nzchar(settings$endDate)) {
      dateArguments$endDate <- settings$endDate
    }
    designArguments$studyWindow <- do.call(CohortIncidence::createDateRange, dateArguments)
  }

  return(do.call(CohortIncidence::createIncidenceDesign, designArguments))
}


computeCohortIncidenceAnalyses <- function(
    connectionDetails = NULL,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema,
    outcomeTable = targetTable,
    characterizationDatabaseSchema,
    characterizationTable,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    cdmDatabaseSchema,
    settings,
    databaseId = "database 1",
    outputFolder,
    minCellCount = 0,
    progressBar = interactive(),
    executionId,
    ...) {

   ensure_installed("CohortIncidence")

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  # check inputs
  errorMessages <- checkmate::makeAssertCollection()
  .checkConnectionDetails(connectionDetails, errorMessages)
  .checkCohortDetails(
    cohortDatabaseSchema = targetDatabaseSchema,
    cohortTable = targetTable,
    type = "target",
    errorMessages = errorMessages
  )
  .checkCohortDetails(
    cohortDatabaseSchema = outcomeDatabaseSchema,
    cohortTable = outcomeTable,
    type = "outcome",
    errorMessages = errorMessages
  )
  .checkTempEmulationSchema(
    tempEmulationSchema = tempEmulationSchema,
    errorMessages = errorMessages
  )

  valid <- checkmate::reportAssertions(errorMessages)

  if (valid) {
    ciDesign <- createCohortIncidenceDesign(settings)


  # run CI
  ParallelLogger::logInfo("Starting CohortIncidence analysis")
  executeResults <- CohortIncidence::executeAnalysis(
    connectionDetails = connectionDetails,

    buildOptions = CohortIncidence::buildOptions(
      refId = settings$refId,
      sourceName = as.character(databaseId),

      #resultsDatabaseSchema = ,
      useTempTables = TRUE,
      cdmDatabaseSchema = cdmDatabaseSchema,

      outcomeCohortTable = paste0(outcomeDatabaseSchema, '.', outcomeTable),
      cohortTable = paste0(characterizationDatabaseSchema, '.', characterizationTable)

        ),

    incidenceDesign = ciDesign,
    resultType = "andromeda"

      )

  ParallelLogger::logInfo(
    "CohortIncidence analysis completed; starting Characterization export"
  )

  # executeAnalysis() creates a temporary, disk-backed Andromeda object. It is
  # saved in the job's work folder below and closed by saveAndromeda(). Ensure
  # it is also closed when processing or saving fails.
  on.exit({
    if (Andromeda::isValidAndromeda(executeResults)) {
      Andromeda::close(executeResults)
    }
  }, add = TRUE)

  # download to CSV files
  createDirectory(file.path(outputFolder))

  exportCohortIncidence(
    executeResults = executeResults,
    databaseId = databaseId,
    exportFolder = file.path(outputFolder),
    irDesign = ciDesign,
    executionId = executionId,
    refId = settings$refId
  )


    return(invisible(TRUE))
  }
}


enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(data[, fieldName]) & data[, fieldName] < minValues & data[, fieldName] != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor) / nrow(data), 1)
    message(
      "   censoring ", sum(toCensor), " values (", percent,
      "%) from ", fieldName, " because value below minimum"
    )
  }
  data[toCensor, fieldName] <- -minValues
  return(data)
}

exportCohortIncidence <- function(
    executeResults,
    databaseId,
    exportFolder,
    irDesign,
    executionId,
    refId
    ){
  if (!inherits(executeResults, "Andromeda")) {
    stop("executeResults must be an Andromeda object")
  }

  result <- executeResults
  sourceTableNames <- names(executeResults)
  ParallelLogger::logInfo(
    "Starting CohortIncidence export of ", length(sourceTableNames),
    " table(s): ", paste(sourceTableNames, collapse = ", ")
  )

  for (tableIndex in seq_along(sourceTableNames)) {
    tableName <- sourceTableNames[[tableIndex]]
    tableData <- executeResults[[tableName]]
    rowCount <- tableData %>%
      dplyr::count() %>%
      dplyr::pull()
    formattedRowCount <- format(
      rowCount,
      big.mark = ",",
      scientific = FALSE,
      trim = TRUE
    )

    outputTableName <- SqlRender::snakeCaseToCamelCase(tableName)
    ParallelLogger::logInfo(
      "Preparing CohortIncidence table ", tableIndex, " of ",
      length(sourceTableNames), ": ", tableName, " -> ", outputTableName,
      " (", formattedRowCount, " rows, ", ncol(tableData), " columns)"
    )

    tableData <- tableData %>%
      dplyr::rename_with(
        .fn = function(columnNames) {
          columnNames[columnNames == "target_cohort_definition_id"] <- "characterization_target_id"
          SqlRender::snakeCaseToCamelCase(columnNames)
        }
      )

    if (outputTableName == "incidenceSummary") {
      tableData <- tableData %>%
        dplyr::mutate(
          databaseId = !!databaseId,
          settingId = !!executionId
        )
    } else {
      tableData <- tableData %>%
        dplyr::mutate(settingId = !!executionId)
    }

    # Assigning an Andromeda table executes the transformation in DuckDB, so
    # only the current table is copied on disk and is not materialized in R.
    ParallelLogger::logInfo(
      "Materializing transformed CohortIncidence table: ", outputTableName
    )
    result[[outputTableName]] <- tableData
    if (outputTableName != tableName) {
      result[[tableName]] <- NULL
    }
    ParallelLogger::logInfo(
      "Finished CohortIncidence table: ", outputTableName
    )
  }

  # in addition to the output of the module, we will produce a T-O lookup table that can be used to filter results
  # to either 'Outcomes for T' or 'Targets for Outcomes'
  ParallelLogger::logInfo("Creating CohortIncidence target-outcome reference table")
  addTargetOutcomeReference(
    andromeda = result,
    irDesign = irDesign,
    refId = refId,
    settingId = executionId
  )
  ParallelLogger::logInfo("Finished CohortIncidence target-outcome reference table")

  resultFile <- file.path(exportFolder, "result")
  ParallelLogger::logInfo("Saving CohortIncidence Andromeda result to ", resultFile)
  saveCharacterizationAndromeda(
    andromeda = result,
    outputFolder = exportFolder
  )
  ParallelLogger::logInfo("Finished saving CohortIncidence Andromeda result")

}


addTargetOutcomeReference <- function(
    andromeda,
    irDesign,
    refId,
    settingId,
    batchSize = 100000) {
  wroteRows <- FALSE
  needsDistinct <- length(irDesign$analysisList) > 1

  for (analysisIndex in seq_along(irDesign$analysisList)) {
    analysis <- irDesign$analysisList[[analysisIndex]]
    outcomeDefs <- Filter(
      function(outcome) outcome$id %in% analysis$outcomes,
      irDesign$outcomeDefs
    )
    outcomeCohortIds <- vapply(outcomeDefs, function(outcome) outcome$cohortId, numeric(1))
    needsDistinct <- needsDistinct || anyDuplicated(analysis$targets) > 0 ||
      anyDuplicated(outcomeCohortIds) > 0

    if (length(analysis$targets) == 0 || length(outcomeCohortIds) == 0) {
      ParallelLogger::logInfo(
        "Skipping target-outcome reference analysis ", analysisIndex, " of ",
        length(irDesign$analysisList), " because it has no targets or outcomes"
      )
      next
    }

    targetsPerBatch <- max(1, floor(batchSize / length(outcomeCohortIds)))
    batchCount <- ceiling(length(analysis$targets) / targetsPerBatch)
    pairCount <- as.double(length(analysis$targets)) * length(outcomeCohortIds)
    ParallelLogger::logInfo(
      "Target-outcome reference analysis ", analysisIndex, " of ",
      length(irDesign$analysisList), ": ",
      format(length(analysis$targets), big.mark = ",", trim = TRUE), " targets x ",
      format(length(outcomeCohortIds), big.mark = ",", trim = TRUE), " outcomes = ",
      format(pairCount, big.mark = ",", scientific = FALSE, trim = TRUE),
      " rows in ", format(batchCount, big.mark = ",", trim = TRUE), " batch(es)"
    )

    for (batchIndex in seq_len(batchCount)) {
      firstTargetIndex <- (batchIndex - 1) * targetsPerBatch + 1
      lastTargetIndex <- min(batchIndex * targetsPerBatch, length(analysis$targets))
      targetIds <- analysis$targets[firstTargetIndex:lastTargetIndex]
      targetOutcomeRef <- expand.grid(
        characterizationTargetId = targetIds,
        outcomeCohortId = outcomeCohortIds,
        KEEP.OUT.ATTRS = FALSE,
        stringsAsFactors = FALSE
      )
      targetOutcomeRef$refId <- refId
      targetOutcomeRef$settingId <- settingId

      if (!wroteRows) {
        andromeda$targetOutcomeRef <- targetOutcomeRef
        wroteRows <- TRUE
      } else {
        Andromeda::appendToTable(
          tbl = andromeda$targetOutcomeRef,
          data = targetOutcomeRef
        )
      }
    }
  }

  if (!wroteRows) {
    andromeda$targetOutcomeRef <- data.frame(
      characterizationTargetId = numeric(),
      outcomeCohortId = numeric(),
      refId = numeric(),
      settingId = character()
    )
  } else if (needsDistinct) {
    # Multiple analyses can contain the same target-outcome pair. Perform the
    # de-duplication in DuckDB instead of constructing all pairs in memory.
    ParallelLogger::logInfo(
      "De-duplicating CohortIncidence target-outcome reference table in DuckDB"
    )
    andromeda$targetOutcomeRef <- andromeda$targetOutcomeRef %>%
      dplyr::distinct()
    ParallelLogger::logInfo(
      "Finished de-duplicating CohortIncidence target-outcome reference table"
    )
  }

  return(invisible(andromeda))
}


# TODO FINISH THIS:
# code that takes a characterizationSettings list, extracts
# cohortIncidence settings and then converts into distinct jobs
# based on the number of threads
getCohortIncidenceJobs <- function(
    characterizationSettings,
    nTargetJobs # not used in this
    ) {
  characterizationSettings <- characterizationSettings$cohortIncidenceSettings
  if (length(characterizationSettings) == 0) {
    return(NULL)
  }

  # recreate settings
  settings <- c()
  for (i in 1:length(characterizationSettings)) {
    settings <- rbind(
      settings,
      data.frame(
        functionName = "computeCohortIncidenceAnalyses",
        settings = as.character(ParallelLogger::convertSettingsToJson(
          c(characterizationSettings[[i]], list(refId = i))
        )),
        executionFolder = paste0("ci_", i),
        jobId = paste0("ci_", i)
      )
    )
  }

  return(settings)
}
