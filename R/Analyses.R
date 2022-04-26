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

#' Create a list of characterization analyses
#'
#' @param ... Objects generated using 'create...Args()' functions, for example `createComputeIncidenceRatesArgs()`.
#'
#' @return
#'
#' @export
createCharacterizationAnalysisList <- function(...) {
  analysisList <- list(...)

  errorMessages <- checkmate::makeAssertCollection()
  for (i in 1:length(analysisList)) {
    checkmate::assertClass(analysisList[[i]], "args", add = errorMessages)
  }
  checkmate::reportAssertions(collection = errorMessages)

  # TODO: Figure out way to establishing type if there is more than one:
  for (i in 1:length(analysisList)) {
    analysisList[[i]]$characterizationType <- "incidence rate"
  }
  class(analysisList) <- "CharacterizationAnalysisList"
  return(analysisList)
}


#' Save a characterization analysis list to file
#'
#' @description
#' Write a `CharacterizationAnalysisList` object to file. The file is in JSON format.
#'
#' @param characterizationAnalysisList   An object of type `CharacterizationAnalysisList` as created using the
#'                                       `createCharacterizationAnalysisList()` function to be written to file.
#' @param file                           The name of the file where the results will be written
#'
#' @export
saveCharacterizationAnalysisList <- function(characterizationAnalysisList, file) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(characterizationAnalysisList, "CharacterizationAnalysisList", add = errorMessages)
  checkmate::assertCharacter(file, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  ParallelLogger::logTrace("Saving CharacterizationAnalysisList to ", file)
  # Required so list doesn't get names:
  class(characterizationAnalysisList) <- "list"
  ParallelLogger::saveSettingsToJson(characterizationAnalysisList, file)
}

#' Load a characterization analysis list from file
#'
#' @description
#' Load a `CharacterizationAnalysisList` object from file. The file is in JSON format.
#'
#' @param file   The name of the file
#'
#' @return
#' A `CharacterizationAnalysisList` object.
#'
#' @export
loadCharacterizationAnalysisList <- function(file) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(file, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  ParallelLogger::logTrace("Loading CharacterizationAnalysisList from ", file)
  characterizationAnalysisList <- ParallelLogger::loadSettingsFromJson(file)
  class(characterizationAnalysisList) <- "CharacterizationAnalysisList"
  return(characterizationAnalysisList)
}
