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

.checkConnection <- function(connection, errorMessages) {
  checkmate::assertClass(connection, "DatabaseConnectorConnection", add = errorMessages)
}

.checkTargetComparatorTables <- function(targetDatabaseSchema,
                                         targetTable,
                                         comparatorDatabaseSchema,
                                         comparatorTable,
                                         errorMessages) {
  checkmate::assertCharacter(targetDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(targetTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(comparatorDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(comparatorTable, len = 1, add = errorMessages)
}

.checkTargetOutcomes <- function(targetOutcomes, errorMessages) {
  checkmate::assertDataFrame(targetOutcomes,
                             min.rows = 1,
                             add = errorMessages)
}

.checkTimeAtRisk <- function(riskWindowStart,
                             startAnchor,
                             riskWindowEnd,
                             endAnchor,
                             errorMessages) {
  checkmate::assertInt(riskWindowStart, add = errorMessages)
  checkmate::assertChoice(startAnchor, c("cohort start", "cohort end"), add = errorMessages)
  checkmate::assertInt(riskWindowEnd, add = errorMessages)
  checkmate::assertChoice(endAnchor, c("cohort start", "cohort end"), add = errorMessages)
}

.checkTempEmulationSchema <- function(tempEmulationSchema, errorMessages) {
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
}
