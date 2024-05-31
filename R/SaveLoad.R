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

colnamesLower <- function(data) {
  colnames(data) <- tolower(
    x = colnames(data)
  )
  return(data)
}

#' export the TimeToEvent results as csv
#'
#' @param result  The output of running \code{computeTimeToEventAnalyses()}
#' @template saveDirectory
#' @param minCellCount  The minimum value that will be displayed in count columns
#'
#' @return
#' A string specifying the directory the csv results are saved to
#'
#' @export
exportTimeToEventToCsv <- function(
    result,
    saveDirectory,
    minCellCount = 0) {
  if (!dir.exists(saveDirectory)) {
    dir.create(
      path = saveDirectory,
      recursive = T
    )
  }

  Andromeda::batchApply(
    tbl = result$timeToEvent,
    fun = function(x) {
      append <- file.exists(
        file.path(
          saveDirectory,
          "time_to_event.csv"
        )
      )

      dat <- as.data.frame(
        x %>%
          dplyr::collect()
      )

      colnames(dat) <- SqlRender::camelCaseToSnakeCase(
        string = colnames(dat)
      )

      if (sum(dat$NUM_EVENTS < minCellCount) > 0) {
        ParallelLogger::logInfo(paste0("Removing NUM_EVENTS less than ", minCellCount))
        dat$NUM_EVENTS[dat$NUM_EVENTS < minCellCount] <- -1
      }

      readr::write_csv(
        x = dat,
        file = file.path(
          saveDirectory,
          "time_to_event.csv"
        ),
        append = append
      )
    }
  )

  invisible(
    file.path(
      saveDirectory,
      "time_to_event.csv"
    )
  )
}


#' export the DechallengeRechallenge results as csv
#'
#' @param result  The output of running \code{computeDechallengeRechallengeAnalyses()}
#' @template saveDirectory
#' @param minCellCount  The minimum value that will be displayed in count columns
#'
#' @return
#' A string specifying the directory the csv results are saved to
#'
#' @export
exportDechallengeRechallengeToCsv <- function(
    result,
    saveDirectory,
    minCellCount = 0) {
  countN <- dplyr::pull(
    dplyr::count(result$dechallengeRechallenge)
  )
  message("Writing ", countN, " rows to csv")

  Andromeda::batchApply(
    tbl = result$dechallengeRechallenge,
    fun = function(x) {
      append <- file.exists(
        file.path(
          saveDirectory,
          "dechallenge_rechallenge.csv"
        )
      )
      dat <- as.data.frame(
        x %>%
          dplyr::collect()
      )

      colnames(dat) <- SqlRender::camelCaseToSnakeCase(
        string = colnames(dat)
      )

      removeInd <- dat$NUM_EVENTS < minCellCount
      if (sum(removeInd) > 0) {
        ParallelLogger::logInfo(paste0("Removing NUM_EVENTS counts less than ", minCellCount))
        if (sum(removeInd) > 0) {
          dat$NUM_CASES[removeInd] <- -1
        }
      }

      removeInd <- dat$DECHALLENGE_ATTEMPT < minCellCount
      if (sum(removeInd) > 0) {
        ParallelLogger::logInfo(paste0("Removing DECHALLENGE_ATTEMPT counts less than ", minCellCount))
        if (sum(removeInd) > 0) {
          dat$DECHALLENGE_ATTEMPT[removeInd] <- -1
        }
      }

      removeInd <- dat$DECHALLENGE_FAIL < minCellCount | dat$DECHALLENGE_SUCCESS < minCellCount
      if (sum(removeInd) > 0) {
        ParallelLogger::logInfo(paste0("Removing DECHALLENGE FAIL or SUCCESS counts less than ", minCellCount))
        if (sum(removeInd) > 0) {
          dat$DECHALLENGE_FAIL[removeInd] <- -1
          dat$DECHALLENGE_SUCCESS[removeInd] <- -1
        }
      }

      removeInd <- dat$RECHALLENGE_ATTEMPT < minCellCount
      if (sum(removeInd) > 0) {
        ParallelLogger::logInfo(paste0("Removing RECHALLENGE_ATTEMPT counts less than ", minCellCount))
        if (sum(removeInd) > 0) {
          dat$RECHALLENGE_ATTEMPT[removeInd] <- -1
        }
      }

      removeInd <- dat$RECHALLENGE_FAIL < minCellCount | dat$RECHALLENGE_SUCCESS < minCellCount
      if (sum(removeInd) > 0) {
        ParallelLogger::logInfo(paste0("Removing RECHALLENGE FAIL or SUCCESS counts less than ", minCellCount))
        if (sum(removeInd) > 0) {
          dat$RECHALLENGE_FAIL[removeInd] <- -1
          dat$RECHALLENGE_SUCCESS[removeInd] <- -1
        }
      }

      readr::write_csv(
        x = dat,
        file = file.path(
          saveDirectory,
          "dechallenge_rechallenge.csv"
        ),
        append = append
      )
    }
  )

  invisible(
    file.path(
      saveDirectory,
      "dechallenge_rechallenge.csv"
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
    saveDirectory) {
  if (!dir.exists(saveDirectory)) {
    dir.create(
      path = saveDirectory,
      recursive = T
    )
  }

  countN <- dplyr::pull(
    dplyr::count(result$rechallengeFailCaseSeries)
  )

  message("Writing ", countN, " rows to csv")

  Andromeda::batchApply(
    tbl = result$rechallengeFailCaseSeries,
    fun = function(x) {
      append <- file.exists(
        file.path(
          saveDirectory,
          "rechallenge_fail_case_series.csv"
        )
      )

      dat <- as.data.frame(
        x %>%
          dplyr::collect()
      )

      colnames(dat) <- SqlRender::camelCaseToSnakeCase(
        string = colnames(dat)
      )

      readr::write_csv(
        x = dat,
        file = file.path(
          saveDirectory,
          "rechallenge_fail_case_series.csv"
        ),
        append = append
      )
    }
  )

  invisible(
    file.path(
      saveDirectory,
      "rechallenge_fail_case_series.csv"
    )
  )
}
