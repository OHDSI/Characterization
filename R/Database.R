# @file Database.R
#
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

#' Create an sqlite database connection
#' @description
#' This function creates a connection to an sqlite database
#'
#' @details
#' This function creates a sqlite database and connection
#'
#' @param sqliteLocation    The location of the sqlite database
#' @family Database
#' @return
#' Returns the connection to the sqlite database
#'
#' @export
createSqliteDatabase <- function(
    sqliteLocation = tempdir()) {
  sqliteLocation <- file.path(
    sqliteLocation,
    "sqliteCharacterization"
  )

  if (!dir.exists(sqliteLocation)) {
    dir.create(
      path = sqliteLocation,
      recursive = T
    )
  }

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = file.path(sqliteLocation, "sqlite.sqlite")
  )

  return(connectionDetails)
}

#' Upload the results into a result database
#' @description
#' This function uploads results in csv format into a result database
#'
#' @details
#' Calls ResultModelManager uploadResults function to upload the csv files
#'
#' @param connectionDetails    The connection details to the result database
#' @param schema               The schema for the result database
#' @param resultsFolder        The folder containing the csv results
#' @param tablePrefix          A prefix to append to the result tables for the characterization results
#' @param csvTablePrefix      The prefix added to the csv results - default is 'c_'
#' @family Database
#' @return
#' Returns the connection to the sqlite database
#'
#' @export
insertResultsToDatabase <- function(
    connectionDetails,
    schema,
    resultsFolder,
    tablePrefix = "",
    csvTablePrefix = "c_") {
  specLoc <- system.file("settings", "resultsDataModelSpecification.csv",
    package = "Characterization"
  )
  specs <- utils::read.csv(specLoc)
  colnames(specs) <- SqlRender::snakeCaseToCamelCase(colnames(specs))
  specs$tableName <- paste0(csvTablePrefix, specs$tableName)
  ResultModelManager::uploadResults(
    connectionDetails = connectionDetails,
    schema = schema,
    resultsFolder = resultsFolder,
    tablePrefix = tablePrefix,
    specifications = specs,
    purgeSiteDataBeforeUploading = F
  )

  return(invisible(NULL))
}

## TODO add this into the csv exporting
removeMinCell <- function(
    data,
    minCellCount = 0,
    minCellCountColumns = list()) {
  for (columns in minCellCountColumns) {
    ind <- apply(
      X = data[, columns, drop = FALSE],
      MARGIN = 1,
      FUN = function(x) sum(x < minCellCount) > 0
    )

    if (sum(ind) > 0) {
      ParallelLogger::logInfo(
        paste0(
          "Removing values less than ",
          minCellCount,
          " from ",
          paste(columns, collapse = " and ")
        )
      )
      data[ind, columns] <- -1
    }
  }
  return(data)
}


#' Create the results tables to store characterization results into a database
#' @description
#' This function executes a large set of SQL statements to create tables that can store results
#'
#' @details
#' This function can be used to create (or delete) Characterization result tables
#'
#' @param connectionDetails            The connectionDetails to a database created by using the
#'                                     function \code{createConnectDetails} in the
#'                                     \code{DatabaseConnector} package.
#' @param resultSchema                 The name of the database schema that the result tables will be created.
#' @param targetDialect                The database management system being used
#' @param deleteExistingTables         If true any existing tables matching the Characterization result tables names will be deleted
#' @param createTables                 If true the Characterization result tables will be created
#' @param tablePrefix                  A string appended to the Characterization result tables
#' @param tempEmulationSchema          The temp schema used when the database management system is oracle
#' @family Database
#' @return
#' Returns NULL but creates the required tables into the specified database schema.
#'
#' @export
createCharacterizationTables <- function(
    connectionDetails,
    resultSchema,
    targetDialect = "postgresql",
    deleteExistingTables = T,
    createTables = T,
    tablePrefix = "c_",
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  errorMessages <- checkmate::makeAssertCollection()
  .checkTablePrefix(
    tablePrefix = tablePrefix,
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(errorMessages)

  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))

  alltables <- tolower(
    DatabaseConnector::getTableNames(
      connection = conn,
      databaseSchema = resultSchema
    )
  )
  tables <- getResultTables()
  tables <- paste0(tablePrefix, tables)

  # adding this to not create tables if all tables esist
  if (sum(tables %in% alltables) == length(tables) & !deleteExistingTables) {
    message("All tables exist so no need to recreate")
    createTables <- FALSE
  }

  if (deleteExistingTables) {
    message("Deleting existing tables")

    for (tb in tables) {
      if (tb %in% alltables) {
        sql <- "DELETE FROM @my_schema.@table"
        sql <- SqlRender::render(
          sql = sql,
          my_schema = resultSchema,
          table = tb
        )
        sql <- SqlRender::translate(
          sql = sql,
          targetDialect = targetDialect,
          tempEmulationSchema = tempEmulationSchema
        )
        DatabaseConnector::executeSql(
          connection = conn,
          sql = sql
        )

        sql <- "DROP TABLE @my_schema.@table"
        sql <- SqlRender::render(
          sql = sql,
          my_schema = resultSchema,
          table = tb
        )
        sql <- SqlRender::translate(
          sql = sql,
          targetDialect = targetDialect,
          tempEmulationSchema = tempEmulationSchema
        )
        DatabaseConnector::executeSql(
          connection = conn,
          sql = sql
        )
      }
    }
  }

  if (createTables) {
    ParallelLogger::logInfo("Creating characterization results tables")
    renderedSql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "ResultTables.sql",
      packageName = "Characterization",
      dbms = targetDialect,
      tempEmulationSchema = tempEmulationSchema,
      my_schema = resultSchema,
      table_prefix = tablePrefix
    )

    DatabaseConnector::executeSql(
      connection = conn,
      sql = renderedSql
    )

    # add database migration here in the future
    migrateDataModel(
      connectionDetails = connectionDetails,
      databaseSchema = resultSchema,
      tablePrefix = tablePrefix
    )
  }
}


migrateDataModel <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  ParallelLogger::logInfo("Migrating data set")
  migrator <- getDataMigrator(
    connectionDetails = connectionDetails,
    databaseSchema = databaseSchema,
    tablePrefix = tablePrefix
  )
  migrator$executeMigrations()
  migrator$finalize()

  ParallelLogger::logInfo("Updating version number")
  updateVersionSql <- SqlRender::loadRenderTranslateSql("UpdateVersionNumber.sql",
    packageName = utils::packageName(),
    database_schema = databaseSchema,
    table_prefix = tablePrefix,
    dbms = connectionDetails$dbms
  )

  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  DatabaseConnector::executeSql(connection, updateVersionSql)
}


getDataMigrator <- function(connectionDetails, databaseSchema, tablePrefix = "") {
  ResultModelManager::DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = databaseSchema,
    tablePrefix = tablePrefix,
    migrationPath = "migrations",
    packageName = utils::packageName()
  )
}

getResultTables <- function() {
  return(
    unique(
      c(
        readr::read_csv(
          file = system.file(
            "settings",
            "resultsDataModelSpecification.csv",
            package = "Characterization"
          ),
          show_col_types = FALSE
        )$table_name,
        "migration", "package_version"
      )
    )
  )
}



# Removes scientific notation for any columns that are
# formatted as doubles. Based on this GitHub issue:
# https://github.com/tidyverse/readr/issues/671#issuecomment-300567232
formatDouble <- function(x, scientific = F, ...) {
  doubleCols <- vapply(x, is.double, logical(1))
  x[doubleCols] <- lapply(x[doubleCols], format, scientific = scientific, ...)

  return(x)
}
