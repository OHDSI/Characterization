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

colnamesLower <- function(data) {
  colnames(data) <- tolower(
    x = colnames(data)
  )
  return(data)
}

#' Save the TimeToEvent results
#'
#' @param result  The output of running \code{computeTimeToEventAnalyses()}
#' @template FileName
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveTimeToEventAnalyses <- function(
    result,
    fileName
) {
  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = fileName,
    maintainConnection = T
  )

  invisible(fileName)
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
) {
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

#' Load the TimeToEvent results
#'
#' @template FileName
#'
#' @return
#' A data.frame with the TimeToEvent results
#'
#' @export
loadTimeToEventAnalyses <- function(fileName) {
  result <- Andromeda::loadAndromeda(fileName)
  return(result)
}

#' Save the DechallengeRechallenge results
#'
#' @param result  The output of running \code{computeDechallengeRechallengeAnalyses()}
#' @template FileName
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveDechallengeRechallengeAnalyses <- function(
    result,
    fileName
) {
  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = fileName,
    maintainConnection = T
  )

  invisible(fileName)
}


#' Save the RechallengeFailCaseSeries results
#'
#' @param result  The output of running \code{computeRechallengeFailCaseSeriesAnalyses()}
#' @template FileName
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveRechallengeFailCaseSeriesAnalyses <- function(
    result,
    fileName
) {
  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = fileName,
    maintainConnection = T
  )

  invisible(fileName)
}


#' Load the DechallengeRechallenge results
#'
#' @template FileName
#'
#' @return
#' A data.frame with the DechallengeRechallenge results
#'
#' @export
loadDechallengeRechallengeAnalyses <- function(
    fileName
) {
  result <- Andromeda::loadAndromeda(fileName)
  return(result)
}

#' Load the RechallengeFailCaseSeries results
#'
#' @template FileName
#'
#' @return
#' A data.frame with the RechallengeFailCaseSeries results
#'
#' @export
loadRechallengeFailCaseSeriesAnalyses <- function(
    fileName
) {
  result <- Andromeda::loadAndromeda(fileName)
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
) {

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
    saveDirectory
) {
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

#' Save the AggregateCovariate results
#'
#' @param result  The output of running \code{computeAggregateCovariateAnalyses()}
#' @template FileName
#'
#' @return
#' A string specifying the directory the results are saved to
#'
#' @export
saveAggregateCovariateAnalyses <- function(
    result,
    fileName
) {
  Andromeda::saveAndromeda(
    andromeda = result,
    fileName = fileName,
    maintainConnection = T
  )

  invisible(fileName)
}

#' Load the AggregateCovariate results
#'
#' @template FileName
#'
#' @return
#' A list of data.frames with the AggregateCovariate results
#'
#' @export
loadAggregateCovariateAnalyses <- function(
    fileName
) {
  result <- Andromeda::loadAndromeda(
    fileName = fileName
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
) {
  if (!dir.exists(saveDirectory)) {
    dir.create(saveDirectory, recursive = T)
  }

  # settings
  Andromeda::batchApply(
    tbl = result$settings,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "settings.csv"
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
          "settings.csv"
        ),
        append = append
      )

    }
  )
  # cohort details
  Andromeda::batchApply(
    tbl = result$cohortCounts,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "cohort_counts.csv"
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
          "cohort_counts.csv"
        ),
        append = append
      )

    }
  )

  # cohort details
  Andromeda::batchApply(
    tbl = result$cohortDetails,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "cohort_details.csv"
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
          "cohort_details.csv"
        ),
        append = append
      )

    }
  )

  # analysisRef
  Andromeda::batchApply(
    tbl = result$analysisRef,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "analysis_ref.csv"
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
          "analysis_ref.csv"
        ),
        append = append
      )

    }
  )

  # covariateRef
  Andromeda::batchApply(
    tbl = result$covariateRef,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "covariate_ref.csv"
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
          "covariate_ref.csv"
        ),
        append = append
      )

    }
  )

  # covariates
  Andromeda::batchApply(
    tbl = result$covariates,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "covariates.csv"
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
          "covariates.csv"
        ),
        append = append
      )

    }
  )

  # covariatesContinuous
  Andromeda::batchApply(
    tbl = result$covariatesContinuous,
    fun = function(x) {

      append <- file.exists(
        file.path(
          saveDirectory,
          "covariates_continuous.csv"
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
          "covariates_continuous.csv"
        ),
        append = append
      )

    }
  )
  invisible(
    file.path(
      saveDirectory,
      c("cohort_details.csv",
        "settings.csv",
        "analysis_ref.csv",
        "covariate_ref.csv",
        "covariates.csv",
        "covariates_continuous.csv",
        "cohort_counts.csv"
      )
    )
  )
}
