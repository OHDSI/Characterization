minSizeCharacterizationIds <- function(
    connection,
    tempEmulationSchema,
    characterizationTargetIds,
    minTargetSize,
    cohortDatabaseSchema,
    targetCountTable
){

  sql <- "SELECT characterization_target_id
   FROM @characterization_schema.@target_count_table
   WHERE n_people >= @min_target_size
   AND characterization_target_id in (@characterization_target_ids);
  "

  sql <- SqlRender::render(
    sql = sql,
    characterization_schema = cohortDatabaseSchema,
    target_count_table = targetCountTable,
    min_target_size = minTargetSize,
    characterization_target_ids = paste0(characterizationTargetIds, collapse = ',')
  )

  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema
  )

  res <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE
  )

  return(res)
}


lookupCases <- function(
    connection,
    lookupDatabaseSchema,
    lookupTableName,
    countTable, # new
    tempEmulationSchema = tempEmulationSchema,
    characterizationTargetIds,
    outcomeIds,
    outcomeWashoutDays,
    startAnchor,
    riskWindowStart,
    endAnchor,
    riskWindowEnd,
    minCaseSize # new
){

  sql <- "
   SELECT
   lt.characterization_case_id,
   characterization_target_id,
   outcome_id,
   outcome_washout_days,
   start_anchor,
   risk_window_start,
   end_anchor,
   risk_window_end

   FROM @lookup_schema.@lookup_table lt
   INNER JOIN @lookup_schema.@case_count_table cct
   ON lt.characterization_case_id = cct.characterization_case_id

   WHERE lt.characterization_target_id in (@char_ids)
   AND lt.outcome_id in (@outcome_ids)
   AND lt.outcome_washout_days = @outcome_washout_days
   AND lt.start_anchor = '@start_anchor'
   AND lt.risk_window_start = @risk_window_start
   AND lt.end_anchor = '@end_anchor'
   AND lt.risk_window_end = @risk_window_end
   AND cct.n_people >= @min_case_size
   ;
   "

  sql <- SqlRender::render(
    sql = sql,
    lookup_schema = lookupDatabaseSchema,
    lookup_table = lookupTableName,
    case_count_table = countTable,
    char_ids = paste0(characterizationTargetIds, collapse  = ','),
    outcome_ids = paste0(outcomeIds, collapse  = ','),
    outcome_washout_days = outcomeWashoutDays,
    start_anchor = startAnchor,
    risk_window_start = riskWindowStart,
    end_anchor = endAnchor,
    risk_window_end = riskWindowEnd,
    min_case_size = minCaseSize
  )

  sql <- SqlRender::translate(
    sql = sql,
    targetDialect = attributes(connection)$dbms,
    tempEmulationSchema = tempEmulationSchema
  )

  lookup <- DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE
  )

  return(lookup)
}
