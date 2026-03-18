context("Incremental")

logFolder <- file.path(tempdir(), "log1")
on.exit(unlink(logFolder))
logFolder2 <- file.path(tempdir(), "log2")
on.exit(unlink(logFolder2))
logFolder3 <- file.path(tempdir(), "log3")
on.exit(unlink(logFolder3))
logFolder4 <- file.path(tempdir(), "log4")
on.exit(unlink(logFolder4))
logFolder5 <- file.path(tempdir(), "log5")
on.exit(unlink(logFolder5))
logFolder6 <- file.path(tempdir(), "log6")
on.exit(unlink(logFolder6))

logFolder7 <- file.path(tempdir(), "log7")
on.exit(unlink(logFolder7))

for (folder in c(
  logFolder, logFolder2, logFolder3,
  logFolder4, logFolder5, logFolder6,
  logFolder7
)) {
  if (!dir.exists(folder)) {
    dir.create(folder, recursive = TRUE)
  }
}

test_that("createIncrementalLog", {
  createIncrementalLog(
    executionFolder = logFolder,
    logname = "execution.csv"
  )
  testthat::expect_true("execution.csv" %in% dir(logFolder))
  executionLog <- utils::read.csv(file.path(logFolder, "execution.csv"))
  testthat::expect_true(nrow(executionLog) == 1)
  testthat::expect_true(executionLog$job_id == 0)

  createIncrementalLog(
    executionFolder = logFolder,
    logname = "madeup.csv"
  )
  testthat::expect_true("madeup.csv" %in% dir(logFolder))
})


# add test for checkIncrementalFilesExist
test_that("checkIncrementalFilesExist", {

  # should return false as no csv files
  testthat::expect_false(checkIncrementalFilesExist(executionFolder = logFolder7))

  # cleanIncremental should fail unless ignoreWhenEmpty is TRUE
  testthat::expect_error(cleanIncremental(executionFolder = logFolder7, ignoreWhenEmpty = FALSE))
  testthat::expect_true(is.null(cleanIncremental(executionFolder = logFolder7, ignoreWhenEmpty = TRUE)))

  # now add the csvs and check should be true
  createIncrementalLog(
    executionFolder = logFolder7,
    logname = "execution.csv"
  )
  createIncrementalLog(
    executionFolder = logFolder7,
    logname = "completed.csv"
  )

  testthat::expect_true(checkIncrementalFilesExist(
    executionFolder = logFolder7
  ))
  # make sure it runs now there are results
  testthat::expect_true(is.null(cleanIncremental(executionFolder = logFolder7, ignoreWhenEmpty = FALSE)))
  testthat::expect_true(is.null(cleanIncremental(executionFolder = logFolder7, ignoreWhenEmpty = TRUE)))

})

test_that("loadIncrementalFiles", {
  # should error as not completed.csv
  testthat::expect_error(
    loadIncrementalFiles(
      executionFolder = logFolder
    )
  )

  # now create the completed.csv
  createIncrementalLog(
    executionFolder = logFolder,
    logname = "completed.csv"
  )

  result <- loadIncrementalFiles(
    executionFolder = logFolder
  )
  testthat::expect_true(sum(c("executed", "completed") %in% names(result)) == 2)
  testthat::expect_true(nrow(result$executed) == 1)
  testthat::expect_true(nrow(result$completed) == 1)
})

test_that("getExecutionJobIssues", {
  result <- loadIncrementalFiles(
    executionFolder = logFolder
  )
  # should error as not completed.csv
  issues <- getExecutionJobIssues(
    executed = result$executed,
    completed = result$completed
  )
  testthat::expect_true(length(issues) == 0)

  # now add some executed but not completed results
  issues <- getExecutionJobIssues(
    executed = data.frame(
      run_date_time = c(1, 1),
      job_id = c(1, 2),
      start_time = c(1, 2),
      end_time = c(1, 2)
    ),
    completed = data.frame(
      run_date_time = c(1),
      job_id = c(1),
      start_time = c(1),
      end_time = c(1)
    )
  )
  testthat::expect_true(issues == 2)

  issues <- getExecutionJobIssues(
    executed = data.frame(
      run_date_time = c(1, 1),
      job_id = c(1, 20),
      start_time = c(1, 2),
      end_time = c(1, 2)
    ),
    completed = data.frame(
      run_date_time = c(1),
      job_id = c(1),
      start_time = c(1),
      end_time = c(1)
    )
  )
  testthat::expect_true(issues == 20)
})

test_that("cleanIncremental", {
  # create folder with issues
  createIncrementalLog(
    executionFolder = logFolder2,
    logname = "execution.csv"
  )
  createIncrementalLog(
    executionFolder = logFolder2,
    logname = "completed.csv"
  )

  # add a job into executed that is not in completed
  readr::write_csv(
    x = data.frame(
      run_date_time = c(10),
      job_id = c(1),
      start_time = c(1),
      end_time = c(1)
    ),
    file = file.path(logFolder2, "execution.csv"),
    append = TRUE
  )

  incrementalFiles <- loadIncrementalFiles(
    executionFolder = logFolder2
  )
  issues <- getExecutionJobIssues(
    executed = incrementalFiles$executed,
    completed = incrementalFiles$completed
  )

  testthat::expect_true(nrow(incrementalFiles$executed) == 2)
  testthat::expect_true(nrow(incrementalFiles$completed) == 1)
  testthat::expect_true(length(issues) == 1)

  dir.create(file.path(logFolder2, "1"))
  andromeda <- Andromeda::andromeda()
  Andromeda::saveAndromeda(
    andromeda = andromeda,
    fileName = file.path(logFolder2, "1", "result")

  )

  testthat::expect_true(file.exists(file.path(logFolder2, "1", "result")))

  # run clean to fix issues
  cleanIncremental(
    executionFolder = logFolder2
  )

  # check issues are fixed
  testthat::expect_true(!file.exists(file.path(logFolder2, "1", "result")))
  incrementalFiles <- loadIncrementalFiles(
    executionFolder = logFolder2
  )
  issues <- getExecutionJobIssues(
    executed = incrementalFiles$executed,
    completed = incrementalFiles$completed
  )

  testthat::expect_true(nrow(incrementalFiles$executed) == 1)
  testthat::expect_true(nrow(incrementalFiles$completed) == 1)
  testthat::expect_true(length(issues) == 0)
})

test_that("checkResultFilesIncremental ", {
  # create folder with issues
  createIncrementalLog(
    executionFolder = logFolder3,
    logname = "execution.csv"
  )
  createIncrementalLog(
    executionFolder = logFolder3,
    logname = "completed.csv"
  )

  result <- checkResultFilesIncremental(
    executionFolder = logFolder3
  )
  testthat::expect_true(is.null(result))

  # add a job into executed that is not in completed
  readr::write_csv(
    x = data.frame(
      run_date_time = c(10),
      job_id = c(1),
      start_time = c(1),
      end_time = c(1)
    ),
    file = file.path(logFolder3, "execution.csv"),
    append = TRUE
  )

  testthat::expect_error(Characterization:::checkResultFilesIncremental(
    executionFolder = logFolder3
  ))
})

test_that("checkResultFilesIncremental ", {
  # create folder with issues
  createIncrementalLog(
    executionFolder = logFolder4,
    logname = "execution.csv"
  )
  createIncrementalLog(
    executionFolder = logFolder4,
    logname = "completed.csv"
  )

  # add a job into executed and completed
  readr::write_csv(
    x = data.frame(
      run_date_time = c(10),
      job_id = c(1),
      start_time = c(1),
      end_time = c(1)
    ),
    file = file.path(logFolder4, "execution.csv"),
    append = TRUE
  )
  readr::write_csv(
    x = data.frame(
      run_date_time = c(10),
      job_id = c(1),
      start_time = c(1),
      end_time = c(1)
    ),
    file = file.path(logFolder4, "completed.csv"),
    append = TRUE
  )

  jobs <- findCompletedJobs(logFolder4)
  testthat::expect_true(1 %in% jobs)
})

test_that("recordIncremental ", {
  createIncrementalLog(
    executionFolder = logFolder6,
    logname = "execution.csv"
  )
  execution <- utils::read.csv(
    file = file.path(logFolder6, "execution.csv")
  )
  testthat::expect_true(!"example100" %in% execution$job_id)

  recordIncremental(
    executionFolder = logFolder6,
    runDateTime = Sys.time(),
    jobId = "example100",
    startTime = Sys.time(),
    endTime = Sys.time(),
    logname = "execution.csv"
  )
  executionJobs <- utils::read.csv(
    file = file.path(logFolder6, "execution.csv")
  )
  testthat::expect_true("example100" %in% executionJobs$job_id)

  # test warning if no file
  testthat::expect_warning(
    recordIncremental(
      executionFolder = logFolder6,
      runDateTime = 1,
      jobId = "example100",
      startTime = 1,
      endTime = 1,
      logname = "execution2.csv"
    )
  )
})


test_that("No Incremental works", {
  result <- checkResultFilesNonIncremental(
    executionFolder = logFolder5
  )
  testthat::expect_true(is.null(result))

  dir.create(
    path = file.path(logFolder5, "job_1"),
    recursive = TRUE
  )
  on.exit(unlink(file.path(logFolder5, "job_1")))

  andromeda <- Andromeda::andromeda()
  Andromeda::saveAndromeda(
    andromeda = andromeda,
    fileName = file.path(logFolder5, "job_1", 'result')
      )

  # now there is a csv file it should error
  testthat::expect_error(
    checkResultFilesNonIncremental(
      executionFolder = logFolder5
    )
  )

  # this should clean the folder of any csv files
  cleanNonIncremental(
    executionFolder = logFolder5
  )
  # previously created result should have been deleted
  testthat::expect_true(length(dir(file.path(logFolder5, "job_1"))) == 0)
})
