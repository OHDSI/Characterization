lookupTargets <- function(
  connection,
  lookupDatabaseSchema,
  lookupTableName,
  tempEmulationSchema,
  targetIds = NULL,
  limitToFirstInNDays = NULL,
  minPriorObservation = NULL,
  characterizationTargetId = NULL
){

  sql <- "
   SELECT
   characterization_target_id,
   target_id,
   limit_to_first_in_n_days,
   min_prior_observation

   FROM @lookup_schema.@lookup_table lt
   {@use_char_id}?{
    WHERE lt.characterization_target_id in (@char_ids);
   }:{
   WHERE lt.target_id in (@target_ids)
   AND lt.limit_to_first_in_n_days = @limit_to_first_in_n_days
   AND lt.min_prior_observation = @min_prior_observation;
  }
   "

  sql <- SqlRender::render(
    sql = sql,
    lookup_schema = lookupDatabaseSchema,
    lookup_table = lookupTableName,
    target_ids = paste0(targetIds, collapse  = ','),
    limit_to_first_in_n_days = limitToFirstInNDays,
    min_prior_observation = minPriorObservation,
    use_char_id = !is.null(characterizationTargetId),
    char_ids = paste0(characterizationTargetId, collapse  = ',')
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


lookupCases <- function(
    connection,
    lookupDatabaseSchema,
    lookupTableName,
    tempEmulationSchema = tempEmulationSchema,
    characterizationTargetIds,
    outcomeIds,
    outcomeWashoutDays,
    startAnchor,
    riskWindowStart,
    endAnchor,
    riskWindowEnd
){

  sql <- "
   SELECT
   characterization_case_id,
   characterization_target_id,
   outcome_id,
   outcome_washout_days,
   start_anchor,
   risk_window_start,
   end_anchor,
   risk_window_end

   FROM @lookup_schema.@lookup_table lt
   WHERE characterization_target_id in (@char_ids)
   AND outcome_id in (@outcome_ids)
   AND outcome_washout_days = @outcome_washout_days
   AND start_anchor = '@start_anchor'
   AND risk_window_start = @risk_window_start
   AND end_anchor = '@end_anchor'
   AND risk_window_end = @risk_window_end;
   "

  sql <- SqlRender::render(
    sql = sql,
    lookup_schema = lookupDatabaseSchema,
    lookup_table = lookupTableName,
    char_ids = paste0(characterizationTargetIds, collapse  = ','),
    outcome_ids = paste0(outcomeIds, collapse  = ','),
    outcome_washout_days = outcomeWashoutDays,
    start_anchor = startAnchor,
    risk_window_start = riskWindowStart,
    end_anchor = endAnchor,
    risk_window_end = riskWindowEnd
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
