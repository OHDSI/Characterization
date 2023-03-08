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

.checkConnection <- function(
  connection,
  errorMessages
  ) {
  checkmate::assertClass(
    x = connection,
    classes = "DatabaseConnectorConnection",
    add = errorMessages
    )
}

.checkConnectionDetails <- function(
  connectionDetails,
  errorMessages
  ) {
  if (is(connectionDetails, "connectionDetails")) {
  checkmate::assertClass(
    x = connectionDetails,
    classes = "connectionDetails",
    add = errorMessages
    )
  } else {
    checkmate::assertClass(
      x = connectionDetails,
      classes = "ConnectionDetails",
      add = errorMessages
    )

  }
}

.checkDechallengeRechallengeSettings <- function(
  settings,
  errorMessages
  ) {
  checkmate::assertClass(
    x = settings,
    classes = "dechallengeRechallengeSettings",
    add = errorMessages
    )
}

.checkDechallengeRechallengeSettingsList <- function(
  settings,
  errorMessages
) {

  if(is.null(settings)){
    return()
  }

  if(class(settings) == 'dechallengeRechallengeSettings'){
    settings <- list(settings)
  }

  lapply(settings, function(x){
    checkmate::assertClass(
      x = x,
      classes = "dechallengeRechallengeSettings",
      add = errorMessages
    )
  }
  )
}

.checkTimeToEventSettings <- function(
  settings,
  errorMessages
) {
  checkmate::assertClass(
    x = settings,
    classes = "timeToEventSettings",
    add = errorMessages
  )
}

.checkTimeToEventSettingsList <- function(
  settings,
  errorMessages
) {

  if(is.null(settings)){
    return()
  }

  if(class(settings) == 'timeToEventSettings'){
    settings <- list(settings)
  }

  lapply(settings, function(x){
    checkmate::assertClass(
      x = x,
      classes = "timeToEventSettings",
      add = errorMessages
    )
  }
  )
}

.checkAggregateCovariateSettings <- function(
  settings,
  errorMessages
) {
  checkmate::assertClass(
    x = settings,
    classes = "aggregateCovariateSettings",
    add = errorMessages
  )
}

.checkAggregateCovariateSettingsList <- function(
  settings,
  errorMessages
) {

  if(is.null(settings)){
    return()
  }

  if(class(settings) == 'aggregateCovariateSettings'){
    settings <- list(settings)
  }

  lapply(settings, function(x){
    checkmate::assertClass(
      x = x,
      classes = "aggregateCovariateSettings",
      add = errorMessages
    )
  }
  )
}

.checkCharacterizationSettings <- function(
  settings,
  errorMessages
) {
  checkmate::assertClass(
    x = settings,
    classes = "characterizationSettings",
    add = errorMessages
  )
}

.checkCohortDetails<- function(
  cohortDatabaseSchema,
  cohortTable,
  type = 'cohort',
  errorMessages
  ) {
  checkmate::assertCharacter(
    x = cohortDatabaseSchema,
    len = 1,
    add = errorMessages,
    .var.name = paste0(type, 'DatabaseSchema')
    )
  checkmate::assertCharacter(
    x = cohortTable,
    len = 1,
    add = errorMessages,
    .var.name = paste0(type, 'Table')
    )
}

.checkCohortIds <- function(
  cohortIds,
  type = 'cohort',
  errorMessages
) {
  checkmate::assertNumeric(
    x = cohortIds,
    add = errorMessages,
    .var.name = paste0(type, 'Id')
  )
}

.checkTimeAtRisk <- function(
  riskWindowStart,
  startAnchor,
  riskWindowEnd,
  endAnchor,
  errorMessages
) {
  checkmate::assertInt(riskWindowStart, add = errorMessages)
  checkmate::assertChoice(startAnchor, c("cohort start", "cohort end"), add = errorMessages)
  checkmate::assertInt(riskWindowEnd, add = errorMessages)
  checkmate::assertChoice(endAnchor, c("cohort start", "cohort end"), add = errorMessages)
}

.checkTempEmulationSchema <- function(
  tempEmulationSchema,
  errorMessages
  ) {
  checkmate::assertCharacter(
    x = tempEmulationSchema,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
    )
}


.checkTablePrefix <- function(
    tablePrefix,
    errorMessages
) {
  checkmate::assertCharacter(
    pattern = "[a-zA-Z]_$",
    x = tablePrefix,
    len = 1,
    null.ok = TRUE,
    add = errorMessages
  )
}



.checkCovariateSettings <- function(covariateSettings,
                                    errorMessages) {
  if (class(covariateSettings) == "covariateSettings") {
    checkmate::assertClass(x = covariateSettings,
                           classes = "covariateSettings",
                           add = errorMessages)
  } else {
    for (j in (1:length(covariateSettings))) {
      checkmate::assertClass(x = covariateSettings[[j]],
                             classes = "covariateSettings",
                             add = errorMessages)
    }
  }

}
