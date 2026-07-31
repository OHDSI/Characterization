#' create the study population settings
#'
#' @param targetIds                    A target cohort id or vector of target cohort ids to do the subsetting to
#' @param limitToFirstInNDays          Should only the first exposure in N days per subject be included?
#' @param minPriorObservation          The minimum required continuous observation time prior to index
#'                                     date for a person to be included in the cohort.
#' @param nestingCohortId              A cohort definition id to restrict the target cohort.  Patient in the target cohort
#'                                     are only included if they are also in the nesting cohort at index.
#' @param minAge                       The minimum age required to be in the target at index
#' @param maxAge                       The maximum age required to be in the target at index
#' @param studyStartDate               The earliest date to be included into the target. Date format is 'yyyymmdd'.
#' @param studyEndDate                 The latest date to be included into the target. Date format is 'yyyymmdd'.
#' @param genderConceptIds              A target cohort subject's gender concept to restrict to
#' @family helper
#'
#' @return
#' A data.frame containing all the settings required
#' for creating the study populations of interest
#' @examples
#' # Create study population settings with a washout period of 365 days and
#' # restricted to adults for target dates that occur for the first time in 365 days.
#' populationSettings <- createStudyPopulationSettings(
#'    targetId  = 1,
#'    limitToFirstInNDays = 365,
#'    minPriorObservation = 365,
#'    minAge = 18
#'    )
#' @export
createStudyPopulationSettings <- function(
    targetIds,
    limitToFirstInNDays = 0,
    minPriorObservation = 0,
    nestingCohortId = NULL,
    minAge = NULL,
    maxAge = NULL,
    studyStartDate = NULL,
    studyEndDate = NULL,
    genderConceptIds = NULL
    ) {

  if(!is.null(limitToFirstInNDays)){
    if(!inherits(limitToFirstInNDays, "numeric") & !inherits(limitToFirstInNDays, "integer")){
      stop('minPriorObservation must be numeric')
    }
    if(limitToFirstInNDays < 0){
      stop('limitToFirstInNDays must be 0 or more')
    }
  } else{
    stop('limitToFirstInNDays must be a numeric > 0 not NULL')
  }

  if(!is.null(minPriorObservation)){
    if(!inherits(minPriorObservation, "numeric") & !inherits(minPriorObservation, "integer")){
      stop('minPriorObservation must be numeric')
    }
    if(minPriorObservation < 0){
      stop('minPriorObservation must be 0 or more')
    }
  } else{
    stop('minPriorObservation must be a numeric > 0 not NULL')
  }

  if(!is.null(nestingCohortId)){
    if(!inherits(nestingCohortId, "numeric") & !inherits(nestingCohortId, "integer")){
      stop('nestingCohortId must be numeric or NULL')
    }
  }

  result <- unique(data.frame(
    targetId = targetIds,
    limitToFirstInNDays = limitToFirstInNDays,
    minPriorObservation = minPriorObservation,
    nestingCohortId = replaceNull(nestingCohortId, 0),
    minAge = replaceNull(minAge,0),
    maxAge = replaceNull(maxAge,9999),
    studyStart = replaceNull(studyStartDate,''), #'yyyy/mm/dd',
    studyEnd = replaceNull(studyEndDate,''),
    genderConceptIds = paste0(sort(genderConceptIds), collapse= ',')
  ))

  return(result)
}


replaceNull <- function(value, nullReplacement){
  if(is.null(value)){
    return(nullReplacement)
  } else{
    return(value)
  }
}


# take a list of studyPopulationSettings and remove redundancy
combineStudyPopulationSettings <- function(studyPopulationSettingslist){

  if(inherits(studyPopulationSettingslist, "data.frame")){
    studyPopulationSettingslist <- list(studyPopulationSettingslist)
  }

  for(i in 1:length(studyPopulationSettingslist)){
    if(!'targetId' %in% colnames(studyPopulationSettingslist[[i]])){
      stop('Incorrect studyPopulationSettingslist')
    }
  }

  combined <- unique(do.call('rbind', studyPopulationSettingslist))

  return(combined)
}


