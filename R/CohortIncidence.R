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
#' @template timeAtRisk
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
  .checkTimeAtRisk(
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    errorMessages = errorMessages
  )
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
  tar <- CohortIncidence::createTimeAtRiskDef(
    id = 1,
    startOffset = settings$riskWindowStart,
    startWith = anchorLookup[[settings$startAnchor]],
    endOffset = settings$riskWindowEnd,
    endWith = anchorLookup[[settings$endAnchor]]
  )
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
      tars = 1,
      outcomes = seq_along(outcomeIds) * 10 + 7,
      targets = targetIds
    )),
    tars = list(tar),
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

    incidenceDesign = ciDesign

      )

  # download to CSV files
  createDirectory(file.path(outputFolder))

  exportCohortIncidence(
    executeResults = executeResults,
    databaseId = databaseId,
    exportFolder = file.path(outputFolder),
    irDesign = ciDesign,
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
    refId
    ){
  result <- Andromeda::andromeda()
  for (tableName in names(executeResults)) {
    tableData <- executeResults[[tableName]]
    names(tableData)[names(tableData) == "target_cohort_definition_id"] <- "characterization_target_id"
    names(tableData) <- SqlRender::snakeCaseToCamelCase(names(tableData))
    result[[SqlRender::snakeCaseToCamelCase(tableName)]] <- tableData
  }

  # in addition to the output of the module, we will produce a T-O lookup table that can be used to filter results
  # to either 'Outcomes for T' or 'Targets for Outcomes'

  targetOutcomeDfList <- lapply(irDesign$analysisList, function(analysis) {
    outcomeDefs <- Filter(function(o) o$id %in% analysis$outcomes, irDesign$outcomeDefs)
    outcome_cohort_id <- sapply(outcomeDefs, function(o) o$cohortId)
    as.data.frame(expand.grid(characterization_target_id = analysis$targets, outcome_cohort_id = outcome_cohort_id))
  })

  targetOutcomeRef <- unique(do.call(rbind, targetOutcomeDfList))
  targetOutcomeRef$ref_id <- refId
  names(targetOutcomeRef) <- SqlRender::snakeCaseToCamelCase(names(targetOutcomeRef))
  result$targetOutcomeRef <- targetOutcomeRef

  result <- addDbAndSettings(
    andromeda = result,
    databaseId = databaseId,
    settingId = refId
  )
  saveCharacterizationAndromeda(
    andromeda = result,
    outputFolder = exportFolder
  )

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
