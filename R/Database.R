# @file Database.R
#
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

#' Create an sqlite database connection
#' @description
#' This function creates a connection to an sqlite database
#'
#' @details
#' This function creates a sqlite database and connection
#'
#' @param sqliteLocation    The location of the sqlite database
#'
#' @return
#' Returns the connection to the sqlite database
#'
#' @export
createSqliteDatabase <- function(
    sqliteLocation = tempdir()
){

  sqliteLocation <- file.path(
    sqliteLocation,
    'sqliteCharacterization'
  )

  if(!dir.exists(sqliteLocation )){
    dir.create(
      path = sqliteLocation,
      recursive = T
    )
  }

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sqlite',
    server = file.path(sqliteLocation, 'sqlite.sqlite')
  )
  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )

  return(connection)
}

# move Andromeda to sqlite database
insertAndromedaToDatabase <- function(
    connection,
    databaseSchema,
    tableName,
    andromedaObject,
    tempEmulationSchema,
    bulkLoad = T,
    tablePrefix = 'c_',
    minCellCount = 0,
    minCellCountColumns = list()
){
  errorMessages <- checkmate::makeAssertCollection()
  .checkTablePrefix(
    tablePrefix = tablePrefix,
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(errorMessages)

  message('Inserting Andromeda table into database table ', tablePrefix, tableName)

  Andromeda::batchApply(
    tbl = andromedaObject,
    fun = function(x){

      data <- as.data.frame(x %>% dplyr::collect()) # apply minCellCount
      data <- removeMinCell(
        data = data,
        minCellCount = minCellCount,
        minCellCountColumns = minCellCountColumns
      )

      DatabaseConnector::insertTable(
        connection = connection,
        databaseSchema = databaseSchema,
        tableName = paste0(tablePrefix,tableName),
        data = data,
        dropTableIfExists = F,
        createTable = F,
        tempEmulationSchema = tempEmulationSchema,
        bulkLoad = bulkLoad,
        camelCaseToSnakeCase = T
      )
    }
  )

  return(TRUE)
}

removeMinCell <- function(
    data,
    minCellCount = 0,
    minCellCountColumns = list()
){
  for(columns in minCellCountColumns){
    ind <- apply(
      X = data[,columns, drop = FALSE],
      MARGIN = 1,
      FUN = function(x) sum(x < minCellCount)>0
    )

    if(sum(ind) > 0 ){
      ParallelLogger::logInfo(
        paste0(
          'Removing values less than ',
          minCellCount,
          ' from ',
          paste(columns, collapse = ' and ')
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
#' @param conn                         A connection to a database created by using the
#'                                     function \code{connect} in the
#'                                     \code{DatabaseConnector} package.
#' @param resultSchema                 The name of the database schema that the result tables will be created.
#' @param targetDialect                The database management system being used
#' @param deleteExistingTables         If true any existing tables matching the Characterization result tables names will be deleted
#' @param createTables                 If true the Characterization result tables will be created
#' @param tablePrefix                  A string appended to the Characterization result tables
#' @param tempEmulationSchema          The temp schema used when the database management system is oracle
#'
#' @return
#' Returns NULL but creates the required tables into the specified database schema.
#'
#' @export
createCharacterizationTables <- function(
    conn,
    resultSchema,
    targetDialect = 'postgresql',
    deleteExistingTables = T,
    createTables = T,
    tablePrefix = 'c_',
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
){
  errorMessages <- checkmate::makeAssertCollection()
  .checkTablePrefix(
    tablePrefix = tablePrefix,
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(errorMessages)


  if(deleteExistingTables){
    message('Deleting existing tables')
    tables <- getResultTables()
    tables <- paste0(tablePrefix, tables)

    alltables <- tolower(
      DatabaseConnector::getTableNames(
        connection = conn,
        databaseSchema = resultSchema
      )
    )

    for(tb in tables){
      if(tb %in% alltables){
        sql <- 'DELETE FROM @my_schema.@table'
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

        sql <- 'DROP TABLE @my_schema.@table'
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

  if(createTables){
    ParallelLogger::logInfo('Creating characterization results tables')
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
  }

}

#' Exports all tables in the result database to csv files
#' @description
#' This function extracts the database tables into csv files
#'
#' @details
#' This function extracts the database tables into csv files
#'
#' @param connectionDetails         The connection details to input into the
#'                                     function \code{connect} in the
#'                                     \code{DatabaseConnector} package.
#' @param resultSchema                 The name of the database schema that the result tables will be created.
#' @param targetDialect                DEPRECATED: derived from \code{connectionDetails}.
#' @param tablePrefix                  The table prefix to apply to the characterization result tables
#' @param filePrefix                   The prefix to apply to the files
#' @param tempEmulationSchema          The temp schema used when the database management system is oracle
#' @param saveDirectory                The directory to save the csv results
#' @param minMeanCovariateValue        The minimum mean covariate value (i.e. the minimum proportion for
#'                                     binary covariates) for a covariate to be included in covariate table.
#'                                     Other covariates are removed to save space.
#'
#' @return
#' csv file per table into the saveDirectory
#'
#' @export
exportDatabaseToCsv <- function(
    connectionDetails,
    resultSchema,
    targetDialect = NULL,
    tablePrefix = "c_",
    filePrefix = NULL,
    tempEmulationSchema = NULL,
    saveDirectory,
    minMeanCovariateValue = 0.001
){

  errorMessages <- checkmate::makeAssertCollection()
  .checkConnectionDetails(connectionDetails, errorMessages)
  .checkTablePrefix(
    tablePrefix = tablePrefix,
    errorMessages = errorMessages
  )
  checkmate::reportAssertions(errorMessages)
  if (!is.null(targetDialect)) {
    warning("The targetDialect argument is deprecated")
  }

  if (is.null(filePrefix)) {
    filePrefix = ''
  }

  # connect to result database
  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(
    DatabaseConnector::disconnect(connection)
  )

  # create the folder to save the csv files
  if(!dir.exists(saveDirectory)){
    dir.create(
      path = saveDirectory,
      recursive = T
    )
  }

  # max number of rows extracted at a time
  maxRowCount <- 1e6

  # get the table names using the function in uploadToDatabase.R
  tables <- getResultTables()

  # extract result per table
  for(table in tables){
    ParallelLogger::logInfo(paste0('Exporting rows from ', table, ' to csv file'))
    # get row count and figure out number of loops
    sql <- "select count(*) as N from @resultSchema.@appendtotable@tablename;"
    sql <- SqlRender::render(
      sql = sql,
      resultSchema = resultSchema,
      appendtotable = tablePrefix,
      tablename = table
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = targetDialect,
      tempEmulationSchema = tempEmulationSchema
    )
    countN <- DatabaseConnector::querySql(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = F
    )$N

    # get column names
    sql <- "select * from @resultSchema.@appendtotable@tablename where 1=0;"
    sql <- SqlRender::render(
      sql = sql,
      resultSchema = resultSchema,
      appendtotable = tablePrefix,
      tablename = table
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = targetDialect,
      tempEmulationSchema = tempEmulationSchema
    )
    cnames <- colnames(DatabaseConnector::querySql(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = F
    ))

    inds <- floor(countN/maxRowCount)
    tableAppend = F
    pb = utils::txtProgressBar(min = 0, max = countN, initial = 0)

    for(i in 1:length(inds)){

      startRow <- (i-1)*maxRowCount + 1
      endRow <- min(i*maxRowCount, countN)

      sql <- "select @cnames from
    (select *,
    ROW_NUMBER() OVER(ORDER BY @cnames) AS row
    from @resultSchema.@appendtotable@tablename
    ) temp
    where
    temp.row >= @start_row and
    temp.row <= @end_row;"
      sql <- SqlRender::render(
        sql = sql,
        resultSchema = resultSchema,
        appendtotable = tablePrefix,
        tablename = table,
        cnames = paste(cnames, collapse = ','),
        start_row = startRow,
        end_row = endRow
      )
      sql <- SqlRender::translate(
        sql = sql,
        targetDialect = targetDialect,
        tempEmulationSchema = tempEmulationSchema
      )
      result <- DatabaseConnector::querySql(
        connection = connection,
        sql = sql,
        snakeCaseToCamelCase = F
      )
      result <- formatDouble(result)

      # save the results as a csv
      readr::write_csv(
        x = result,
        file = file.path(saveDirectory, paste0(tolower(filePrefix), table,'.csv')),
        append = tableAppend
      )
      tableAppend = T
      utils::setTxtProgressBar(pb,endRow)

    }
    close(pb)
  }

  invisible(saveDirectory)
}

getResultTables <- function(){
  return(
    unique(
      readr::read_csv(
        file = system.file(
          'settings',
          'resultsDataModelSpecification.csv',
          package = 'Characterization'
        ),
        show_col_types = FALSE
      )$table_name
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

