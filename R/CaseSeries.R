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

#' Create aggregate covariate study settings
#'
#' @param targetIds   A list of cohortIds for the target cohorts
#' @param outcomeIds  A list of cohortIds for the outcome cohorts
#' @param minPriorObservation The minimum time (in days) in the database a patient in the target cohorts must be observed prior to index
#' @param outcomeWashoutDays Patients with the outcome within outcomeWashout days prior to index are excluded from the risk factor analysis
#' @template timeAtRisk
#' @param covariateSettings   An object created using \code{FeatureExtraction::createCovariateSettings}
#' @param caseCovariateSettings An object created using \code{createDuringCovariateSettings}
#' @param casePreTargetDuration    The number of days prior to case index we use for FeatureExtraction
#' @param casePostOutcomeDuration    The number of days prior to case index we use for FeatureExtraction
#' @param extractNonCaseCovariates Whether to extract aggregate covariates and counts for patients in the targets and outcomes in addition to the cases
#' @param minTargetSize The minimum size of the target cohorts for them to have aggregate covariates calculated
#' @param minTwithOSize The minimum size of the cohorts corresponding to patients in the target with the outcome during time-at-risk for them to have aggregate covariates calculated
#' @family Aggregate
#' @return
#' A list with the settings
#'
#' @examples
#'
#' aggregateSetting <- createAggregateCovariateSettings(
#'   targetIds = c(1,2),
#'   outcomeIds = c(3),
#'   minPriorObservation = 365,
#'   outcomeWashoutDays = 90,
#'   riskWindowStart = 1,
#'   startAnchor = "cohort start",
#'   riskWindowEnd = 365,
#'   endAnchor = "cohort start",
#'   casePreTargetDuration = 365,
#'   casePostOutcomeDuration = 365
#' )
#'
#' @export
createAggregateCovariateSettings <- function(
    targetIds,
    outcomeIds,
    minPriorObservation = 0,
    outcomeWashoutDays = 0,
    riskWindowStart = 1,
    startAnchor = "cohort start",
    riskWindowEnd = 365,
    endAnchor = "cohort start",
    caseCovariateSettings = createDuringCovariateSettings(
      useConditionGroupEraDuring = TRUE,
      useDrugGroupEraDuring = TRUE,
      useProcedureOccurrenceDuring = TRUE,
      useDeviceExposureDuring = TRUE,
      useMeasurementDuring = TRUE,
      useObservationDuring = TRUE,
      useVisitConceptCountDuring = TRUE
    ),
    casePreTargetDuration = 365,
    casePostOutcomeDuration = 365,
    minTargetSize = 0,
    minTwithOSize = 0
    ) {
  errorMessages <- checkmate::makeAssertCollection()
  # check targetIds is a vector of int/double
  .checkCohortIds(
    cohortIds = targetIds,
    type = "target",
    errorMessages = errorMessages
  )
  # check outcomeIds is a vector of int/double
  .checkCohortIds(
    cohortIds = outcomeIds,
    type = "outcome",
    errorMessages = errorMessages
  )

  # check TAR - EFF edit
  if (length(riskWindowStart) > 1) {
    stop("Please add one time-at-risk per setting")
  }
  .checkTimeAtRisk(
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    errorMessages = errorMessages
  )

  # check minPriorObservation
  .checkMinPriorObservation(
    minPriorObservation = minPriorObservation,
    errorMessages = errorMessages
  )

  # add check for outcomeWashoutDays

  checkmate::reportAssertions(errorMessages)

  # check unique Ts and Os
  if (length(targetIds) != length(unique(targetIds))) {
    message("targetIds have duplicates - making unique")
    targetIds <- unique(targetIds)
  }
  if (length(outcomeIds) != length(unique(outcomeIds))) {
    message("outcomeIds have duplicates - making unique")
    outcomeIds <- unique(outcomeIds)
  }


  # create list
  result <- list(
    targetIds = targetIds,
    minPriorObservation = minPriorObservation,
    outcomeIds = outcomeIds,
    outcomeWashoutDays = outcomeWashoutDays,
    riskWindowStart = riskWindowStart,
    startAnchor = startAnchor,
    riskWindowEnd = riskWindowEnd,
    endAnchor = endAnchor,
    caseCovariateSettings = caseCovariateSettings, # case series
    casePreTargetDuration = casePreTargetDuration, # case series
    casePostOutcomeDuration = casePostOutcomeDuration, # case series,
    minTargetSize = minTargetSize,
    minTwithOSize = minTwithOSize
  )

  class(result) <- "caseSeriesSettings"
  return(result)
}

createExecutionIds <- function(size) {
  executionIds <- gsub(" ", "", gsub("[[:punct:]]", "", paste(Sys.time(), sample(1000000, size), sep = "")))
  return(executionIds)
}

# TODO cdmVersion should be in runChar
computeCaseSeriesAnalyses <- function(
    connectionDetails = NULL,
    cdmDatabaseSchema,
    cdmVersion = 5,
    targetDatabaseSchema,
    targetTable,
    outcomeDatabaseSchema = targetDatabaseSchema, # remove
    outcomeTable = targetTable, # remove
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
    settings,
    databaseId = "database 1",
    outputFolder,
    minCharacterizationMean = 0,
    minCellCount = 0,
    progressBar = interactive(),
    ...) {

  if(missing(outputFolder)){
    stop('Please enter a output path value for outputFolder')
  }

  # add code to get characterizationCaseId

  # run FE for the characterizationCaseId*10 + 3/4/5

  # run the case series extraction for binary

  # run the case series extraction for continuous

  # extract the covariate_ref and analysis_ref

  # export the andromeda to csv

  # clean up

  return(invisible(TRUE))
}

getCaseSeriesJobs <- function(
    characterizationSettings,
    threads) {

  return(NULL)
}


