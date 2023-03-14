#' viewCharacterization - Interactively view the characterization results
#'
#' @description
#' This is a shiny app for viewing interactive plots and tables
#' @details
#' Input is the output of ...
#' @param resultLocation   The location of the results
#' @param cohortDefinitionSet  The cohortDefinitionSet extracted using webAPI
#' @return
#' Opens a shiny app for interactively viewing the results
#'
#' @export
viewCharacterization <- function(
    resultLocation,
    cohortDefinitionSet = NULL
    ) {

  databaseSettings <- prepareCharacterizationShiny(
    resultLocation = resultLocation,
    cohortDefinitionSet = cohortDefinitionSet
  )

  viewChars(databaseSettings)
}

prepareCharacterizationShiny <- function(
    resultLocation,
    cohortDefinitionSet
    ){
  server <- file.path(resultLocation, 'sqliteCharacterization', 'sqlite.sqlite')

  connectionDetailsSettings <- list(
    dbms = 'sqlite',
    server = server
  )

  connectionDetails <- do.call(
    what = DatabaseConnector::createConnectionDetails,
    args = connectionDetailsSettings
  )

  con <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))

  tables <- tolower(DatabaseConnector::getTableNames(con, 'main'))

  if(!'cg_cohort_definition' %in% tables){
    cohortIds <- unique(
      c(
        DatabaseConnector::querySql(con, 'select distinct TARGET_COHORT_ID from c_cohort_details where TARGET_COHORT_ID != 0;')$TARGET_COHORT_ID,
        DatabaseConnector::querySql(con, 'select distinct OUTCOME_COHORT_ID from c_cohort_details where OUTCOME_COHORT_ID != 0;')$OUTCOME_COHORT_ID,
        DatabaseConnector::querySql(con, 'select distinct TARGET_COHORT_DEFINITION_ID from c_time_to_event;')$TARGET_COHORT_DEFINITION_ID,
        DatabaseConnector::querySql(con, 'select distinct OUTCOME_COHORT_DEFINITION_ID from c_time_to_event;')$OUTCOME_COHORT_DEFINITION_ID,
        DatabaseConnector::querySql(con, 'select distinct TARGET_COHORT_DEFINITION_ID from c_rechallenge_fail_case_series;')$TARGET_COHORT_DEFINITION_ID,
        DatabaseConnector::querySql(con, 'select distinct OUTCOME_COHORT_DEFINITION_ID from c_rechallenge_fail_case_series;')$OUTCOME_COHORT_DEFINITION_ID
      )
    )

    DatabaseConnector::insertTable(
      connection = con,
      databaseSchema = 'main',
      tableName = 'cg_COHORT_DEFINITION',
      data = data.frame(
        cohortDefinitionId = cohortIds,
        cohortName = getCohortNames(cohortIds, cohortDefinitionSet)
      ),
      camelCaseToSnakeCase = T
    )
  }

  if(!'database_meta_data' %in% tables){
    dbIds <- unique(
      c(
        DatabaseConnector::querySql(con, 'select distinct DATABASE_ID from c_analysis_ref;')$DATABASE_ID,
        DatabaseConnector::querySql(con, 'select distinct DATABASE_ID from c_dechallenge_rechallenge;')$DATABASE_ID,
        DatabaseConnector::querySql(con, 'select distinct DATABASE_ID from c_time_to_event;')$DATABASE_ID
      )
    )

    DatabaseConnector::insertTable(
      connection = con,
      databaseSchema = 'main',
      tableName = 'DATABASE_META_DATA',
      data = data.frame(
        databaseId = dbIds,
        cdmSourceAbbreviation = paste0('database ', dbIds)
      ),
      camelCaseToSnakeCase = T
    )
  }

  if(!'i_incidence_summary' %in% tables){

    x <- c("refId", "databaseId", "sourceName",
           "targetCohortDefinitionId", "targetName", "tarId",
           "tarStartWith", "tarStartOffset", "tarEndWith", "tarEndOffset",
           "subgroupId", 'subgroupName',
           'outcomeId','outcomeCohortDefinitionId', 'outcomeName',
           'clean_window',
           'ageId', 'ageGroupName',
           'genderId', 'genderName',
           'startYear', 'personsAtRiskPe', 'personsAtRisk',
           'personDaysPe', 'personDays',
           'personOutcomesPe', 'personOutcomes',
           'outcomesPe', 'outcomes',
           'incidenceProportionP100p',
           'incidenceRateP100py'
    )
    df <- data.frame(matrix(ncol = length(x), nrow = 0))
    colnames(df) <- x

    DatabaseConnector::insertTable(
      connection = con,
      databaseSchema = 'main',
      tableName = 'i_incidence_summary',
      data = df,
      camelCaseToSnakeCase = T
    )
  }

  databaseSettings <- list(
    connectionDetailsSettings = connectionDetailsSettings,
    schema = 'main',
    tablePrefix = 'c_',
    cohortTablePrefix = 'cg_',
    incidenceTablePrefix = 'i_',
    databaseTable = 'DATABASE_META_DATA'
  )

  return(databaseSettings)
}

viewChars <- function(
    databaseSettings,
    testApp = F
    ){
  ensure_installed("ShinyAppBuilder")
  ensure_installed("ResultModelManager")

  connectionDetails <- do.call(
    DatabaseConnector::createConnectionDetails,
    databaseSettings$connectionDetailsSettings
  )
  connection <- ResultModelManager::ConnectionHandler$new(connectionDetails)
  databaseSettings$connectionDetailsSettings <- NULL

  # set database settings into system variables
  Sys.setenv("resultDatabaseDetails_characterization" = as.character(ParallelLogger::convertSettingsToJson(databaseSettings)))

  config <- ParallelLogger::loadSettingsFromJson(
    fileName = system.file(
      'shinyConfig.json',
      package = "Characterization"
    )
  )

  if(!testApp){
    ShinyAppBuilder::viewShiny(config = config, connection = connection)
  } else{
    ShinyAppBuilder::createShinyApp(config = config, connection = connection)
  }
}



getCohortNames <- function(cohortIds, cohortDefinitionSet){

  if(!is.null(cohortDefinitionSet)){
  cohortNames <- sapply(
    cohortIds,
    function(x){
      cohortDefinitionSet$cohortName[cohortDefinitionSet$cohortId == x]
    }
  )
  } else{
    cohortNames <- paste0('cohort ', cohortIds)
  }

  return(cohortNames)
}


# Borrowed from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L44
is_installed <- function (pkg, version = 0) {
  installed_version <- tryCatch(utils::packageVersion(pkg),
                                error = function(e) NA)
  !is.na(installed_version) && installed_version >= version
}

# Borrowed and adapted from devtools: https://github.com/hadley/devtools/blob/ba7a5a4abd8258c52cb156e7b26bb4bf47a79f0b/R/utils.r#L74
ensure_installed <- function(pkg) {
  if (!is_installed(pkg)) {
    msg <- paste0(sQuote(pkg), " must be installed for this functionality.")
    if (interactive()) {
      message(msg, "\nWould you like to install it?")
      if (utils::menu(c("Yes", "No")) == 1) {
        if(pkg%in%c("ShinyAppBuilder", "ResultModelManager")){

          # add code to check for devtools...
          dvtCheck <- tryCatch(utils::packageVersion('devtools'),
                               error = function(e) NA)
          if(is.na(dvtCheck)){
            utils::install.packages('devtools')
          }

          devtools::install_github(paste0('OHDSI/',pkg))
        }else{
          utils::install.packages(pkg)
        }
      } else {
        stop(msg, call. = FALSE)
      }
    } else {
      stop(msg, call. = FALSE)
    }
  }
}
