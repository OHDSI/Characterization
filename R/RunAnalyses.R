# Copyright 2022 Observational Health Data Sciences and Informatics
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

#' Run multiple characterization analyses
#'
#' @param connectionDetails   An object of type `connectionDetails` as created using the
#'                            [DatabaseConnector::createConnectionDetails()] function.
#' @template TargetoutcomeTables
#' @template TempEmulationSchema
#' @param databaseId                    A unique database identifier that will be included in all output tables.
#' @param characterizationAnalysisList  An object of type `CharacterizationAnalysisList` as created using the
#'                                      `createCharacterizationAnalysisList()` function.
#' @param exportFolder                  A folder where results will be written.
#'
#' @return
#'
#'
#' @export
runCharacterizationAnalyses <- function(connectionDetails = NULL,
                                        targetDatabaseSchema,
                                        targetTable,
                                        outcomeDatabaseSchema,
                                        outcomeTable,
                                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                        databaseId,
                                        characterizationAnalysisList,
                                        exportFolder) {
  start <- Sys.time()
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "connectionDetails", add = errorMessages)
  .checkTargetOutcomeTables(targetDatabaseSchema,
                            targetTable,
                            outcomeDatabaseSchema,
                            outcomeTable,
                            errorMessages)
  .checkTempEmulationSchema(tempEmulationSchema, errorMessages)
  checkmate::assertClass(characterizationAnalysisList, "CharacterizationAnalysisList", add = errorMessages)
  checkmate::assertCharacter(exportFolder, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  if (dir.exists(exportFolder)) {
    toDelete <- list.files(path = exportFolder, pattern = ".csv")
    if (length(toDelete) > 0) {
      warning(sprintf("Existing CSV files detected in '%s'. Deleting before generating new ones.", exportFolder))
      unlink(file.path(exportFolder, toDelete))
    }
  } else {
    dir.create(exportFolder, recursive = TRUE)
  }

  # Running all analyses
  globalArgs <- list(connection = connection,
                     targetDatabaseSchema = targetDatabaseSchema,
                     targetTable = targetTable,
                     outcomeDatabaseSchema = outcomeDatabaseSchema,
                     outcomeTable = outcomeTable,
                     tempEmulationSchema = tempEmulationSchema)
  incidenceRateAnalysisId <- 1
  for (i in 1:length(characterizationAnalysisList)) {
    args <- append(globalArgs, characterizationAnalysisList[[i]])
    type <- args$characterizationType
    args$characterizationType <- NULL
    if (type == "incidence rate") {
      results <- do.call(computeIncidenceRates, args)
      results$databaseId <- databaseId
      results$incidenceRateAnalysisId <- incidenceRateAnalysisId

      fileName <- file.path(exportFolder, "incidence_rate.csv")
      colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
      readr::write_csv(results, fileName, append = file.exists(fileName))

      analysisReference <- characterizationAnalysisList[[i]]
      analysisReference <- tibble(analysisId = incidenceRateAnalysisId,
                                  riskWindowStart = analysisReference$riskWindowStart,
                                  startAnchor = analysisReference$startAnchor,
                                  riskWindowEnd = analysisReference$riskWindowEnd,
                                  endAnchor = analysisReference$endAnchor)
      fileName <- file.path(exportFolder, "incidence_rate_analysis.csv")
      colnames(incidenceRateAnalyses) <- SqlRender::camelCaseToSnakeCase(colnames(incidenceRateAnalyses))
      readr::write_csv(incidenceRateAnalyses, fileName, append = file.exists(fileName))

      incidenceRateAnalysisId <- incidenceRateAnalysisId + 1
    }
  }
}
