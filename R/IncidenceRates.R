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

#' Compute incidence rates
#'
#' @template Connection
#' @template TargetComparatorTables
#' @template TargetOutcomes
#' @template TimeAtRisk
#' @template TempEmulationSchema
#'
#' @return
#' The incidence rates of the outcome cohorts in the target cohorts.
#'
#' @export
computeIncidenceRates <- function(connection,
                                  targetDatabaseSchema,
                                  targetTable,
                                  comparatorDatabaseSchema,
                                  comparatorTable,
                                  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                  targetOutcomes,
                                  riskWindowStart = 0,
                                  startAnchor = "cohort start",
                                  riskWindowEnd = 0,
                                  endAnchor = "cohort end") {
  errorMessages <- checkmate::makeAssertCollection()
  .checkConnection(connection, errorMessages)
  .checkTargetComparatorTables(targetDatabaseSchema,
                               targetTable,
                               comparatorDatabaseSchema,
                               comparatorTable,
                               errorMessages)
  .checkTargetOutcomes(targetOutcomes, errorMessages)
  .checkTimeAtRisk(riskWindowStart,
                   startAnchor,
                   riskWindowEnd,
                   endAnchor,
                   errorMessages)
  .checkTempEmulationSchema(tempEmulationSchema, errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  start <- Sys.time()

  message("Uploading target-outcomes")
  DatabaseConnector::insertTable(connection = connection,
                                 tableName = "#target_outcome",
                                 data = targetOutcomes,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = TRUE,
                                 tempEmulationSchema = tempEmulationSchema,
                                 progressBar = FALSE,
                                 camelCaseToSnakeCase = TRUE)

  message("Computing incidence rates")
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "ComputeIncidenceRates.sql",
                                           packageName = "Characterization",
                                           dbms = connection@dbms,
                                           tempEmulationSchema = tempEmulationSchema,
                                           target_database_schema = targetDatabaseSchema,
                                           target_table = targetTable,
                                           comparator_database_schema = comparatorDatabaseSchema,
                                           comparator_table = comparatorTable,
                                           target_outcome_table = "#target_outcome",
                                           risk_window_start = riskWindowStart,
                                           start_anchor = startAnchor,
                                           risk_window_end = riskWindowEnd,
                                           end_anchor = endAnchor)
  DatabaseConnector::executeSql(connection = connection, sql = sql)

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetIncidenceRates.sql",
                                           packageName = "Characterization",
                                           dbms = connection@dbms,
                                           tempEmulationSchema = tempEmulationSchema)
  result <- DatabaseConnector::querySql(connection = connection, sql = sql, snakeCaseToCamelCase = TRUE)

  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "DropIncidenceRates.sql",
                                           packageName = "Characterization",
                                           dbms = connection@dbms,
                                           tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection = connection, sql = sql, progressBar = FALSE, reportOverallTime = FALSE)

  delta <- Sys.time() - start
  message("Computing incidence rates took ", signif(delta, 3), " ", attr(delta, "units"))
  return(result)
}
