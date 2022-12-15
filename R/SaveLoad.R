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

colnamesLower <- function(data){
  colnames(data) <- tolower(colnames(data))
  return(data)
}

#' Save the TimeToEvent results
#'
#' @param result  The output of running \code{computeTimeToEventAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveTimeToEventAnalyses <- function(
  result,
  saveDirectory
  ){

  folderLoc <- file.path(saveDirectory, 'TimeToEvent')

  if(!dir.exists(saveDirectory)){
    dir.create(saveDirectory, recursive = T)
  }

  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = folderLoc,
    maintainConnection = T
    )

  return(invisible(saveDirectory))
}

#' export the TimeToEvent results as csv
#'
#' @param result  The output of running \code{computeTimeToEventAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the csv results are saved to
#'
#' @export
exportTimeToEventToCsv <- function(
  result,
  saveDirectory
){

  if(!dir.exists(saveDirectory)){
    dir.create(saveDirectory, recursive = T)
  }

  Andromeda::batchApply(
    tbl = result$timeToEvent,
    fun = function(x){
      CohortGenerator::writeCsv(
        x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
        file = file.path(saveDirectory, 'time_to_event.csv'),
        append = T
      )
    }
  )

  return(
    invisible(
      file.path(
        saveDirectory,
        'time_to_event.csv'
        )
      )
    )
}

#' Load the TimeToEvent results
#'
#' @template saveDirectory
#'
#' @return
#' A data.frame with the TimeToEvent results
#'
#' @export
loadTimeToEventAnalyses <- function(saveDirectory){

  folderLoc <- file.path(saveDirectory, 'TimeToEvent')

  result <- Andromeda::loadAndromeda(folderLoc)

  return(result)
}

#' Save the DechallengeRechallenge results
#'
#' @param result  The output of running \code{computeDechallengeRechallengeAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveDechallengeRechallengeAnalyses <- function(
  result,
  saveDirectory
  ){

  folderLoc <- file.path(saveDirectory, 'DechallengeRechallenge')

  if(!dir.exists(folderLoc)){
    dir.create(folderLoc, recursive = T)
  }

  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = folderLoc,
    maintainConnection = T
  )

  return(invisible(saveDirectory))
}


#' Save the RechallengeFailCaseSeries results
#'
#' @param result  The output of running \code{computeRechallengeFailCaseSeriesAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveRechallengeFailCaseSeriesAnalyses <- function(
  result,
  saveDirectory
){

  folderLoc <- file.path(saveDirectory, 'RechallengeFailedCaseSeries')

  if(!dir.exists(folderLoc)){
    dir.create(folderLoc, recursive = T)
  }

  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = folderLoc,
    maintainConnection = T
  )

  return(invisible(saveDirectory))
}


#' Load the DechallengeRechallenge results
#'
#' @template saveDirectory
#'
#' @return
#' A data.frame with the DechallengeRechallenge results
#'
#' @export
loadDechallengeRechallengeAnalyses <- function(
  saveDirectory
){

  folderLoc <- file.path(saveDirectory, 'DechallengeRechallenge')

  result <- Andromeda::loadAndromeda(folderLoc)

  return(result)

}

#' Load the RechallengeFailCaseSeries results
#'
#' @template saveDirectory
#'
#' @return
#' A data.frame with the RechallengeFailCaseSeries results
#'
#' @export
loadRechallengeFailCaseSeriesAnalyses <- function(
  saveDirectory
){

  folderLoc <- file.path(saveDirectory, 'RechallengeFailCaseSeries')

  result <- Andromeda::loadAndromeda(folderLoc)

  return(result)
}

#' export the DechallengeRechallenge results as csv
#'
#' @param result  The output of running \code{computeDechallengeRechallengeAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the csv results are saved to
#'
#' @export
exportDechallengeRechallengeToCsv <- function(
  result,
  saveDirectory
){

  if(!dir.exists(saveDirectory)){
    dir.create(saveDirectory, recursive = T)
  }

  countN <- result$dechallengeRechallenge %>% dplyr::tally()
  ParallelLogger::logInfo(paste0('Writing ', ifelse(is.null(countN$n),0,countN$n), ' rows to csv'))

    Andromeda::batchApply(
      tbl = result$dechallengeRechallenge,
      fun = function(x){
        CohortGenerator::writeCsv(
          x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
          file = file.path(saveDirectory, 'dechallenge_rechallenge.csv'),
          append = T
        )
      }
    )


  return(
    invisible(
      file.path(
        saveDirectory,
        'dechallenge_rechallenge.csv'
      )
    )
  )
}

#' export the RechallengeFailCaseSeries results as csv
#'
#' @param result  The output of running \code{computeRechallengeFailCaseSeriesAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the csv results are saved to
#'
#' @export
exportRechallengeFailCaseSeriesToCsv <- function(
  result,
  saveDirectory
){

  if(!dir.exists(saveDirectory)){
    dir.create(saveDirectory, recursive = T)
  }

  countN <- result$rechallengeFailCaseSeries %>%
    dplyr::tally()

  ParallelLogger::logInfo(paste0('Writing ', ifelse(is.null(countN$n),0,countN$n), ' rows to csv'))

    Andromeda::batchApply(
      tbl = result$rechallengeFailCaseSeries,
      fun = function(x){
        CohortGenerator::writeCsv(
          x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
          file = file.path(saveDirectory, 'rechallenge_fail_case_series.csv'),
          append = T
        )
      }
    )

  return(
    invisible(
      file.path(
        saveDirectory,
        'rechallenge_fail_case_series.csv'
      )
    )
  )
}

#' Save the AggregateCovariate results
#'
#' @param result  The output of running \code{computeAggregateCovariateAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveAggregateCovariateAnalyses <- function(
  result,
  saveDirectory
){

  folderLoc <- file.path(saveDirectory, 'AggregateCovariate')

  if(!dir.exists(saveDirectory )){
    dir.create(saveDirectory, recursive = T)
  }

  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = folderLoc,
    maintainConnection = T
  )

  return(invisible(saveDirectory))
}

#' Load the AggregateCovariate results
#'
#' @template saveDirectory
#'
#' @return
#' A list of data.frames with the AggregateCovariate results
#'
#' @export
loadAggregateCovariateAnalyses <- function(
  saveDirectory
){

  folderLoc <- file.path(saveDirectory, 'AggregateCovariate')

  result <- Andromeda::loadAndromeda(
    fileName = file.path(saveDirectory, 'AggregateCovariate')
    )

  return(result)
}

#' export the AggregateCovariate results as csv
#'
#' @param result  The output of running \code{computeAggregateCovariateAnalyses()}
#' @template saveDirectory
#'
#' @return
#' A string specifying the directory the csv results are saved to
#'
#' @export
exportAggregateCovariateToCsv <- function(
  result,
  saveDirectory
){

  if(!dir.exists(saveDirectory)){
    dir.create(saveDirectory, recursive = T)
  }

  # analysisRef
  Andromeda::batchApply(
    tbl = result$analysisRef,
    fun = function(x){
      CohortGenerator::writeCsv(
        x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
        file = file.path(saveDirectory, 'analysis_ref.csv'),
        append = T
      )
    }
  )

  # covariateRef
  Andromeda::batchApply(
    tbl = result$covariateRef,
    fun = function(x){
      CohortGenerator::writeCsv(
        x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
        file = file.path(saveDirectory, 'covariate_ref.csv'),
        append = T
      )
    }
  )

  # covariates
  Andromeda::batchApply(
    tbl = result$covariates,
    fun = function(x){
      CohortGenerator::writeCsv(
        x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
        file = file.path(saveDirectory, 'covariates.csv'),
        append = T
      )
    }
  )

  # covariatesContinuous
  Andromeda::batchApply(
    tbl = result$covariatesContinuous,
    fun = function(x){
      CohortGenerator::writeCsv(
        x = colnamesLower(as.data.frame(x %>% dplyr::collect())),
        file = file.path(saveDirectory, 'covariates_continuous.csv'),
        append = T
      )
    }
  )
  return(
    invisible(
      file.path(
        saveDirectory,
        c('analysis_ref.csv','covariate_ref.csv','covariates.csv','covariates_continuous.csv')
      )
    )
  )
}
