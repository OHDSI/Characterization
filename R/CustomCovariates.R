#' Create during covariate settings
#'
#' @details
#' creates an object specifying how during covariates should be constructed from data in the CDM model.
#'
#' @param useConditionOccurrenceDuring                         One covariate per condition in the
#'                                                             condition_occurrence table starting between
#'                                                             cohort start and cohort end. (analysis ID 109)
#' @param useConditionOccurrencePrimaryInpatientDuring         One covariate per condition observed as
#'                                                             a primary diagnosis in an inpatient
#'                                                             setting in the condition_occurrence table starting between
#'                                                             cohort start and cohort end. (analysis ID 110)
#' @param useConditionEraDuring                                One covariate per condition in the condition_era table
#'                                                             starting between cohort start and cohort end.
#'                                                             (analysis ID 217)
#' @param useConditionGroupEraDuring                           One covariate per condition era rolled
#'                                                             up to groups in the condition_era table
#'                                                             starting between cohort start and cohort end.
#'                                                             (analysis ID 218)
#' @param useDrugExposureDuring                                One covariate per drug in the drug_exposure table between cohort start and end.
#'                                                             (analysisId 305)
#' @param useDrugEraDuring                                     One covariate per drug in the drug_era table between cohort start and end.
#'                                                             (analysis ID 417)
#' @param useDrugGroupEraDuring                                One covariate per drug rolled up to ATC groups in the drug_era table between cohort start and end.
#'                                                             (analysis ID 418)
#' @param useProcedureOccurrenceDuring                         One covariate per procedure in the procedure_occurrence table between cohort start and end.
#'                                                             (analysis ID 505)
#' @param useDeviceExposureDuring                              One covariate per device in the device exposure table starting between cohort start and end.
#'                                                             (analysis ID 605)
#' @param useMeasurementDuring                                 One covariate per measurement in the measurement table between cohort start and end.
#'                                                             (analysis ID 713)
#' @param useObservationDuring                                 One covariate per observation in the observation table between cohort start and end.
#'                                                             (analysis ID 805)
#' @param useVisitCountDuring                                  The number of visits observed between cohort start and end.
#'                                                             (analysis ID 926)
#' @param useVisitConceptCountDuring                           The number of visits observed between cohort start and end, stratified by visit concept ID.
#'                                                             (analysis ID 927)
#' @param includedCovariateConceptIds                          A list of concept IDs that should be
#'                                                             used to construct covariates.
#' @param addDescendantsToInclude                              Should descendant concept IDs be added
#'                                                             to the list of concepts to include?
#' @param excludedCovariateConceptIds                          A list of concept IDs that should NOT be
#'                                                             used to construct covariates.
#' @param addDescendantsToExclude                              Should descendant concept IDs be added
#'                                                             to the list of concepts to exclude?
#' @param includedCovariateIds                                 A list of covariate IDs that should be
#'                                                             restricted to.
#' @family CovariateSetting
#'
#' @return
#' An object of type \code{covariateSettings}, to be used in other functions.
#'
#' @examples
#' settings <- createDuringCovariateSettings(
#'   useConditionOccurrenceDuring = TRUE,
#'   useConditionOccurrencePrimaryInpatientDuring = FALSE,
#'   useConditionEraDuring = FALSE,
#'   useConditionGroupEraDuring = FALSE
#' )
#'
#' @export
createDuringCovariateSettings <- function(
    useConditionOccurrenceDuring = FALSE,
    useConditionOccurrencePrimaryInpatientDuring = FALSE,
    useConditionEraDuring = FALSE,
    useConditionGroupEraDuring = FALSE,
    useDrugExposureDuring = FALSE,
    useDrugEraDuring = FALSE,
    useDrugGroupEraDuring = FALSE,
    useProcedureOccurrenceDuring = FALSE,
    useDeviceExposureDuring = FALSE,
    useMeasurementDuring = FALSE,
    useObservationDuring = FALSE,
    useVisitCountDuring = FALSE,
    useVisitConceptCountDuring = FALSE,
    includedCovariateConceptIds = c(),
    addDescendantsToInclude = FALSE,
    excludedCovariateConceptIds = c(),
    addDescendantsToExclude = FALSE,
    includedCovariateIds = c()) {
  covariateSettings <- list(
    temporal = FALSE, # needed?
    temporalSequence = FALSE
  )
  formalNames <- names(formals(createDuringCovariateSettings))
  anyUseTrue <- FALSE
  for (name in formalNames) {
    value <- get(name)
    if (is.null(value)) {
      value <- vector()
    }
    if (grepl("use.*", name)) {
      if (value) {
        covariateSettings[[sub("use", "", name)]] <- value
        anyUseTrue <- TRUE
      }
    } else {
      covariateSettings[[name]] <- value
    }
  }
  if (!anyUseTrue) {
    stop("No covariate analysis selected. Must select at least one")
  }

  attr(covariateSettings, "fun") <- "Characterization::getDbDuringCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}

#' Extracts covariates that occur during a cohort
#'
#' @details
#' The user specifies a what during covariates they want and this executes them using FE
#'
#' @param connection  The database connection
#' @param oracleTempSchema  The temp schema if using oracle
#' @param cdmDatabaseSchema  The schema of the OMOP CDM data
#' @param cdmVersion  version of the OMOP CDM data
#' @param cohortTable  the table name that contains the target population cohort
#' @param rowIdField  string representing the unique identifier in the target population cohort
#' @param aggregated  whether the covariate should be aggregated
#' @param cohortIds  cohort id for the target cohort
#' @param covariateSettings  settings for the covariate cohorts and time periods
#'
#' @template tempEmulationSchema
#' @param targetDatabaseSchema (Optional) The schema to save the tables targetCovariateTable/targetCovariateContinuousTable/targetCovariateRefTable/targetCovariateRefTable/targetAnalysisRefTable
#'                              when they are not temp tables and the output is being exported to database tables.
#' @param targetCovariateTable  (Optional) The name of the table where the resulting binary covariates will
#'                               be stored. If not provided, results will be fetched to R. The table can be
#'                               a permanent table in the \code{targetDatabaseSchema} or a temp table. If
#'                               it is a temp table, do not specify \code{targetDatabaseSchema}.
#' @param targetCovariateContinuousTable  (Optional) The name of the table where the resulting continuous covariates will
#'                               be stored. If not provided, results will be fetched to R. The table can be
#'                               a permanent table in the \code{targetDatabaseSchema} or a temp table. If
#'                               it is a temp table, do not specify \code{targetDatabaseSchema}.
#'
#' @param targetCovariateRefTable (Optional) The name of the table where the covariate reference will be stored.
#'
#' @param targetAnalysisRefTable (Optional) The name of the table where the analysis reference will be stored.
#' @param targetTimeRefTable     (Optional) The name of the table for the time reference
#' @param minCharacterizationMean The minimum mean value for binary characterization output. Values below this will be cut off from output. This
#'                                will help reduce the file size of the characterization output, but will remove information
#'                                on covariates that have very low values. The default is 0.
#'
#'
#' @param dropTableIfExists      If targetDatabaseSchema, drop any existing tables. Otherwise, results are merged
#'                               into existing table data. Overides createTable.
#' @param createTable            Run sql to create table? Code does not check if table exists.
#'
#' @param ...  additional arguments from FeatureExtraction
#'
#' @family CovariateSetting
#'
#' @examples
#'
#' conDet <- exampleOmopConnectionDetails()
#' connection <- DatabaseConnector::connect(conDet)
#'
#' settings <- createDuringCovariateSettings(
#'   useConditionOccurrenceDuring = TRUE,
#'   useConditionOccurrencePrimaryInpatientDuring = FALSE,
#'   useConditionEraDuring = FALSE,
#'   useConditionGroupEraDuring = FALSE
#' )
#'
#' duringData <- getDbDuringCovariateData(
#'   connection <- connection,
#'   cdmDatabaseSchema = 'main',
#'   cohortIds = 1,
#'   covariateSettings = settings,
#'   cohortTable = 'cohort'
#' )
#'
#' DatabaseConnector::disconnect(connection)
#'
#' @return
#' A 'FeatureExtraction' covariateData object containing the during covariates based on user settings
#'
#' @export
getDbDuringCovariateData <- function(
    connection,
    oracleTempSchema = NULL,
    cdmDatabaseSchema,
    cdmVersion = "5",
    cohortTable = "#cohort_person",
    rowIdField = "subject_id",
    aggregated = TRUE,
    cohortIds = c(-1),
    covariateSettings,
    minCharacterizationMean = 0,
    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),

    targetDatabaseSchema = NULL,
    targetCovariateTable = NULL,
    targetCovariateContinuousTable = NULL,
    targetCovariateRefTable = NULL,
    targetAnalysisRefTable = NULL,
    targetTimeRefTable = NULL,
    dropTableIfExists = FALSE,
    createTable = TRUE,

    ...) {
  writeLines("Constructing during cohort covariates")
  if (!aggregated) {
    stop("Only aggregation supported")
  }

  targetTables <- list(
    covariates = targetCovariateTable,
    covariatesContinuous = targetCovariateContinuousTable,
    covariateRef = targetCovariateRefTable,
    analysisRef = targetAnalysisRefTable,
    timeRef = targetTimeRefTable
  )

  # if targetCovariateTable is NULL then extract to Andromeda
  allTempTables <- all(substr(targetTables,1,1) == "#")
  extractToAndromeda <- is.null(targetCovariateTable)

  getDomainSettings <- utils::read.csv(system.file("csv/PrespecAnalyses.csv", package = "Characterization"))

  # not showing progress
  progressBar <- FALSE

  # create Tables
  sql <- "DROP TABLE IF EXISTS #cov_ref;
  CREATE TABLE #cov_ref(
  covariate_id bigint,
  covariate_name varchar(1000),
  analysis_id int,
  concept_id bigint,
  value_as_concept_id int,
  collisions int
  );"
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(connection, sql = sql, progressBar = progressBar)

  sql <- "DROP TABLE IF EXISTS #analysis_ref;
  CREATE TABLE #analysis_ref(
   analysis_id int,
   analysis_name varchar(100),
   domain_id varchar(100),
   start_day varchar(100),
   end_day varchar(100),
   is_binary varchar(1),
   missing_means_zero varchar(1)
  );"
  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = DatabaseConnector::dbms(connection),
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = progressBar)

  # included covariates
  includedCovTable <- ""
  if (length(covariateSettings$includedCovariateIds) > 0) {
    # create-
    includedCovTable <- "#included_cov"
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = includedCovTable,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      data = data.frame(id = covariateSettings$includedCovariateIds),
      camelCaseToSnakeCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = progressBar
    )
  }

  # including concept ids
  includedConceptTable <- ""
  if (length(covariateSettings$includedCovariateConceptIds) > 0) {
    includedConceptTable <- "#include_concepts"
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = includedConceptTable,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      data = data.frame(id = covariateSettings$includedCovariateConceptIds),
      camelCaseToSnakeCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = progressBar
    )

    if (covariateSettings$addDescendantsToInclude) {
      SqlRender::loadRenderTranslateSql(
        sqlFilename = "IncludeDescendants.sql",
        packageName = "Characterization",
        dbms = DatabaseConnector::dbms(connection),
        table_name = includedConceptTable,
        cdm_database_schema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema
      )
    }
  }

  # exlcuding concept ids
  excludedConceptTable <- ""
  if (length(covariateSettings$excludedCovariateConceptIds) > 0) {
    excludedConceptTable <- "#exclude_concepts"
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = excludedConceptTable,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      data = data.frame(id = covariateSettings$excludedCovariateConceptIds),
      camelCaseToSnakeCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      progressBar = progressBar
    )

    if (covariateSettings$addDescendantsToInclude) {
      SqlRender::loadRenderTranslateSql(
        sqlFilename = "IncludeDescendants.sql",
        packageName = "Characterization",
        dbms = DatabaseConnector::dbms(connection),
        table_name = excludedConceptTable,
        cdm_database_schema = cdmDatabaseSchema,
        tempEmulationSchema = tempEmulationSchema
      )
    }
  }

  domainSettingsIndexes <- which(getDomainSettings$analysisName %in% names(covariateSettings))
  i <- 0
  binaryInd <- c()
  continuousInd <- c()
  useBinary <- FALSE
  useContinuous <- FALSE
  result <- Andromeda::andromeda()

  for (domainSettingsIndex in domainSettingsIndexes) {
    i <- i + 1

    if (getDomainSettings$isBinary[domainSettingsIndex] == "Y") {
      binaryInd <- c(i, binaryInd)
      useBinary <- TRUE
    } else {
      continuousInd <- c(i, continuousInd)
      useContinuous <- TRUE
    }
    # Load template sql and fill
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = getDomainSettings$sqlFileName[domainSettingsIndex],
      packageName = "Characterization",
      dbms = attr(connection, "dbms"),
      cohort_table = cohortTable,
      # cohort_ids = cohortIds,
      cohort_definition_id = cohortIds, # added?
      row_id_field = rowIdField,
      cdm_database_schema = cdmDatabaseSchema,
      aggregated = aggregated,
      sub_type = getDomainSettings$subType[domainSettingsIndex],
      analysis_id = getDomainSettings$analysisId[domainSettingsIndex],
      analysis_name = getDomainSettings$analysisName[domainSettingsIndex],
      domain_table = getDomainSettings$domainTable[domainSettingsIndex],
      domain_start_date = getDomainSettings$domainStartDate[domainSettingsIndex],
      domain_end_date = getDomainSettings$domainEndDate[domainSettingsIndex],
      domain_concept_id = getDomainSettings$domainConceptId[domainSettingsIndex],
      domain_id = getDomainSettings$domainId[domainSettingsIndex],
      included_cov_table = includedCovTable,
      included_concept_table = includedConceptTable,
      excluded_concept_table = excludedConceptTable,
      covariate_table = paste0("#cov_", i),
      tempEmulationSchema = tempEmulationSchema
    )
    message(paste0("Executing during sql code for ", getDomainSettings$analysisName[domainSettingsIndex]))
    start <- Sys.time()
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql,
      progressBar = progressBar
    )
    time <- Sys.time() - start
    message(paste0("Execution took ", round(time, digits = 2), " ", units(time)))
  }

  # all_covariates.cohort_definition_id,\n  all_covariates.covariate_id,\n  all_covariates.sum_value,\n  CAST(all_covariates.sum_value / (1.0 * total.total_count) AS FLOAT) AS average_value

  message(paste0("Extracting covariates"))
  start <- Sys.time()
  # Retrieve the covariate:
  if (useBinary) {
    sql <- paste0(
      "{@extract_to_table}?{

      INSERT INTO @target_covariate_table(
      cohort_definition_id,
      covariate_id,
      sum_value,
      average_value
      )
      }", # ADDED NEW
      "select temp.*, CAST(temp.sum_value / (1.0 * total.total_count) AS FLOAT) AS average_value from (",
      paste0(paste0("select cohort_definition_id, covariate_id, sum_value from #cov_", binaryInd), collapse = " union "),
      ") temp inner join
      (SELECT cohort_definition_id, COUNT(*) AS total_count
      FROM @cohort_table {@cohort_definition_id != -1} ? {\nWHERE cohort_definition_id IN (@cohort_definition_id)}
      GROUP BY cohort_definition_id ) total
       on temp.cohort_definition_id = total.cohort_definition_id",
      ";"
    )
    sql <- SqlRender::render(
      sql = sql,
      cohort_table = cohortTable,
      cohort_definition_id = paste0(c(-1), collapse = ","),
      target_covariate_table = targetTables$covariates,
      extract_to_table = !extractToAndromeda
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = DatabaseConnector::dbms(connection),
      tempEmulationSchema = tempEmulationSchema
    )

    if(extractToAndromeda){
      message('Downloading covariates')
      DatabaseConnector::querySqlToAndromeda(
        connection = connection,
        sql = sql,
        andromeda = result,
        andromedaTableName = "covariates",
        appendToTable = FALSE,
        snakeCaseToCamelCase = TRUE
      )
      if (minCharacterizationMean != 0 && "averageValue" %in% colnames(result$covariates)) {

        numRows <- result$covariates %>% dplyr::count() %>% dplyr::pull()
        if(numRows > 0){
          result$covariates <- result$covariates %>%
            dplyr::filter(.data$averageValue >= minCharacterizationMean)
        }
      }
    } else{
      # insert into the table
      message('Inserting covariates into table')
      DatabaseConnector::executeSql(
        connection = connection,
        sql = sql
      )
    }
  }

  if (useContinuous) {
    sql <- paste0(
      "{@extract_to_table}?{

      INSERT INTO @target_covariate_continuous_table(
      cohort_definition_id,
      covariate_id,
      count_value,
	    min_value,
	    max_value,
	    average_value,
	    standard_deviation,
	    median_value,
	    p10_value,
	    p25_value,
	    p75_value,
	    p90_value
      )
      }",
      paste0(paste0("select
                    cohort_definition_id,
      covariate_id,
      count_value,
	    min_value,
	    max_value,
	    average_value,
	    standard_deviation,
	    median_value,
	    p10_value,
	    p25_value,
	    p75_value,
	    p90_value
                    from #cov_", continuousInd), collapse = " union "),

      ";"
    )
    sql <- SqlRender::render(
      sql = sql,
      target_covariate_continuous_table = targetTables$covariatesContinuous,
      extract_to_table = !extractToAndromeda
    )
    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = DatabaseConnector::dbms(connection),
      tempEmulationSchema = tempEmulationSchema
    )
    if(extractToAndromeda){
      message('Downloading continuous covariates')
      DatabaseConnector::querySqlToAndromeda(
        connection = connection,
        sql = sql,
        andromeda = result,
        andromedaTableName = "covariatesContinuous",
        appendToTable = FALSE,
        snakeCaseToCamelCase = TRUE
      )} else{
        message('Inserting covariates into table')
        DatabaseConnector::executeSql(
          connection = connection,
          sql = sql
        )
      }
  }
  # Retrieve the covariate ref:
  if(extractToAndromeda){
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = "select * from #cov_ref;",
      targetDialect = DatabaseConnector::dbms(connection),
      tempEmulationSchema = tempEmulationSchema
    ),
    andromeda = result,
    andromedaTableName = "covariateRef",
    appendToTable = FALSE,
    snakeCaseToCamelCase = TRUE
  )}else{

    sql <- SqlRender::render(
      sql = "
      INSERT INTO @target_covariate_ref(
      covariate_id,
	    covariate_name,
	    analysis_id,
	    concept_id
      )

      SELECT
      covariate_id,
	    covariate_name,
	    analysis_id,
	    concept_id
      FROM #cov_ref;",
      target_covariate_ref = targetTables$covariateRef
    )

    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = DatabaseConnector::dbms(connection),
      tempEmulationSchema = tempEmulationSchema
    )

    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )
  }

  # Retrieve the analysis ref:
  if(extractToAndromeda){
  DatabaseConnector::querySqlToAndromeda(
    connection = connection,
    sql = SqlRender::translate(
      sql = "select * from #analysis_ref;",
      targetDialect = DatabaseConnector::dbms(connection),
      tempEmulationSchema = tempEmulationSchema
    ),
    andromeda = result,
    andromedaTableName = "analysisRef",
    appendToTable = FALSE,
    snakeCaseToCamelCase = TRUE
  )} else{

    sql <- SqlRender::render(
      sql = "
      INSERT INTO @target_analysis_ref(
      analysis_id,
	    analysis_name,
	    domain_id,
	    is_binary,
	    missing_means_zero
      )
      SELECT
      analysis_id,
	    analysis_name,
	    domain_id,
	    is_binary,
	    missing_means_zero
      FROM #analysis_ref;",
      target_analysis_ref = targetTables$analysisRef
    )

    sql <- SqlRender::translate(
      sql = sql,
      targetDialect = DatabaseConnector::dbms(connection),
      tempEmulationSchema = tempEmulationSchema
    )
    DatabaseConnector::executeSql(
      connection = connection,
      sql = sql
    )
  }
  time <- Sys.time() - start
  message(paste0("Extracting covariates took ", round(time, digits = 2), " ", units(time)))

  # clean up: drop tables...
  if (length(c(binaryInd, continuousInd)) != 0) {
    message(paste0("Removing temp covariate tables"))
    for (i in c(binaryInd, continuousInd)) {
      sql <- "TRUNCATE TABLE #cov_@id;\nDROP TABLE #cov_@id;\n"
      sql <- SqlRender::render(
        sql,
        id = i
      )
      sql <- SqlRender::translate(
        sql = sql,
        targetDialect = attr(connection, "dbms"),
        oracleTempSchema = oracleTempSchema,
        tempEmulationSchema = tempEmulationSchema
      )
      DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
  }

  # Construct metaData
  metaData <- list(sql = sql, call = match.call())
  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"
  return(result)
}
