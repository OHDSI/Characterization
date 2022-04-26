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
#' @template TargetComparatorTables
#' @template TempEmulationSchema
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
                                        comparatorDatabaseSchema,
                                        comparatorTable,
                                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                        characterizationAnalysisList,
                                        exportFolder) {
  start <- Sys.time()
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "connectionDetails", add = errorMessages)
  .checkTargetComparatorTables(targetDatabaseSchema,
                               targetTable,
                               comparatorDatabaseSchema,
                               comparatorTable,
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
                     comparatorDatabaseSchema = comparatorDatabaseSchema,
                     comparatorTable = comparatorTable,
                     tempEmulationSchema = tempEmulationSchema)
  for (i in 1:length(characterizationAnalysisList)) {
    args <- append(globalArgs, characterizationAnalysisList[[i]])
    type <- args$characterizationType
    args$characterizationType <- NULL
    if (type == "incidence rate") {
      results <- do.call(computeIncidenceRates, args)
      fileName <- file.path(exportFolder, "incidence_rate.csv")
      colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
      readr::write_csv(results, fileName, append = file.exists(fileName))
    }
  }
}
