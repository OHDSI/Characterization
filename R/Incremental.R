createIncrementalLog <- function(
    executionFolder,
    logname = "execution.csv") {
  if (!dir.exists(executionFolder)) {
    dir.create(executionFolder, recursive = T)
  }

  if (!file.exists(file.path(executionFolder, logname))) {
    x <- data.frame(
      run_date_time = Sys.time(),
      job_id = 0,
      start_time = Sys.time(),
      end_time = Sys.time()
    )
    readr::write_csv(
      x = x,
      file = file.path(executionFolder, logname)
    )
  }
}

# check whether the execution.csv and completed.csv both exist
checkIncrementalFilesExist <- function(executionFolder) {

  incrementalExists <- file.exists(file.path(executionFolder, "execution.csv")) &&
    file.exists(file.path(executionFolder, "completed.csv"))

  # NOTE: what happens if one exists and the other doesnt?
  return(incrementalExists)

}

loadIncrementalFiles <- function(executionFolder) {
  if (file.exists(file.path(executionFolder, "execution.csv"))) {
    executed <- utils::read.csv(file.path(executionFolder, "execution.csv"))
  } else {
    stop("execution.csv missing")
  }

  if (file.exists(file.path(executionFolder, "completed.csv"))) {
    completed <- utils::read.csv(file.path(executionFolder, "completed.csv"))
  } else {
    stop("completed.csv missing")
  }
  return(list(
    executed = executed,
    completed = completed
  ))
}

getExecutionJobIssues <- function(
    executed,
    completed) {
  executedJobs <- unique(executed$job_id)
  completedJobs <- unique(completed$job_id)

  issues <- executedJobs[!executedJobs %in% completedJobs]
  return(issues)
}

#' Removes csv files from folders that have not been marked as completed
#' and removes the record of the execution file
#'
#' @param executionFolder   The folder that has the execution files
#' @param ignoreWhenEmpty   When TRUE, if there are no incremental logs then nothing is run
#' @family Incremental
#' @return
#' A list with the settings
#'
#' @export
cleanIncremental <- function(
    executionFolder,
    ignoreWhenEmpty = FALSE
    ) {

  # check whether incremental files exists
  fileExist <- checkIncrementalFilesExist(executionFolder = executionFolder)

  if(fileExist || !ignoreWhenEmpty){
    incrementalFiles <- loadIncrementalFiles(
      executionFolder
    )

    issues <- getExecutionJobIssues(
      executed = incrementalFiles$executed,
      completed = incrementalFiles$completed
    )

    if (length(issues) > 0) {
      # delete contents inside folder
      for (i in 1:length(issues)) {
        files <- dir(file.path(executionFolder, issues[i]), full.names = T)
        for (file in files) {
          message(paste0("Deleting incomplete result file ", file))
          file.remove(file)
        }
      }
    }

    # now update the execution to remove the issue rows
    executionFile <- utils::read.csv(
      file = file.path(executionFolder, "execution.csv")
    )
    fixedExecution <- executionFile[!executionFile$job_id %in% issues, ]
    utils::write.csv(
      x = fixedExecution,
      file = file.path(executionFolder, "execution.csv")
    )
  }

  return(invisible(NULL))
}

checkResultFilesIncremental <- function(
    executionFolder) {
  incrementalFiles <- loadIncrementalFiles(
    executionFolder
  )

  issues <- getExecutionJobIssues(
    executed = incrementalFiles$executed,
    completed = incrementalFiles$completed
  )

  if (length(issues) > 0) {
    stop(paste0("jobIds: ", paste0(issues, collapse = ","), "executed but not completed.  Please run cleanIncremental() to remove incomplete results."))
  }

  return(invisible(NULL))
}

findCompletedJobs <- function(executionFolder) {
  incrementalFiles <- loadIncrementalFiles(executionFolder)
  return(unique(incrementalFiles$completed$job_id))
}


recordIncremental <- function(
    executionFolder,
    runDateTime,
    jobId,
    startTime,
    endTime,
    logname = "execution.csv") {
  if (file.exists(file.path(executionFolder, logname))) {
    x <- data.frame(
      run_date_time = runDateTime,
      job_id = jobId,
      start_time = startTime,
      end_time = endTime
    )
    readr::write_csv(
      x = x,
      file = file.path(executionFolder, logname),
      append = T
    )
  } else {
    warning(paste0(logname, " file missing so no logging possible"))
  }
}

#' Removes csv files from the execution folder as there should be no csv files
#' when running in non-incremental model
#'
#' @param executionFolder   The folder that has the execution files
#' @family Incremental
#' @return
#' A list with the settings
#'
#' @export
cleanNonIncremental <- function(
    executionFolder) {
  # remove all files from the executionFolder
  files <- dir(
    path = executionFolder,
    recursive = T,
    full.names = T,
    pattern = ".csv"
  )
  if (length(files) > 0) {
    for (file in files) {
      message(paste0("Deleting file ", file))
      file.remove(file)
    }
  }
}

checkResultFilesNonIncremental <- function(
    executionFolder) {
  files <- dir(
    path = executionFolder,
    recursive = T,
    full.names = T,
    pattern = ".csv"
  )
  if (length(files) > 0) {
    errorMessage <- paste0(
      "Running in non-incremental but csv files exist in execution folder.",
      " please delete manually or using cleanNonIncremental()"
    )
    stop(errorMessage)
  }

  return(invisible(NULL))
}
