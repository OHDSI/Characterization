#library(Characterization)
#library(testthat)

context("DechallengeRechallenge")

test_that("createDechallengeRechallengeSettings", {

  targetIds <- sample(x = 100, size = sample(10, 1))
  outcomeIds <- sample(x = 100, size = sample(10, 1))

  res <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
    )

  testthat::expect_true(
   inherits(res, 'dechallengeRechallengeSettings')
  )

  testthat::expect_equal(
    res$targetCohortDefinitionIds,
    targetIds
  )

  testthat::expect_equal(
    res$outcomeCohortDefinitionIds,
    outcomeIds
  )

  testthat::expect_equal(
    res$dechallengeStopInterval,
    30
  )

  testthat::expect_equal(
    res$dechallengeEvaluationWindow,
    31
  )

})

test_that("computeDechallengeRechallengeAnalyses", {

  targetIds <- c(2)
  outcomeIds <- c(3,4)

  res <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 30
  )

  dc <- computeDechallengeRechallengeAnalyses(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = 'main',
    targetTable = 'cohort',
    dechallengeRechallengeSettings = res,
    databaseId = 'testing'
    )

  testthat::expect_true(inherits(dc, 'Andromeda'))
  testthat::expect_true(names(dc) == 'dechallengeRechallenge')

  if(actions){ # causing *** caught segfault *** address 0x68, cause 'memory not mapped' zip::zipr

  # test saving/loading
  saveDechallengeRechallengeAnalyses(
    result = dc,
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_true(
    file.exists(file.path(tempdir(), 'DechallengeRechallenge'))
  )

  res2 <- loadDechallengeRechallengeAnalyses(
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_equal(
    nrow(dc$dechallengeRechallenge %>% collect()),
    nrow(res2$dechallengeRechallenge %>% collect())
    )
  }

  # check exporting to csv
  exportDechallengeRechallengeToCsv(
    result = dc,
    saveDirectory = tempdir()
  )

  countN <- dc$dechallengeRechallenge %>%
    dplyr::tally()

  if(!is.null(countN$n)){
    testthat::expect_true(
      file.exists(file.path(
        tempdir(),
        'dechallenge_rechallenge.csv'
      )
      )
    )
  }



})

test_that("computeRechallengeFailCaseSeriesAnalyses", {

  targetIds <- c(1,2)
  outcomeIds <- c(4)

  res <- createDechallengeRechallengeSettings(
    targetIds = targetIds,
    outcomeIds = outcomeIds,
    dechallengeStopInterval = 30,
    dechallengeEvaluationWindow = 31
  )

  dc <- computeRechallengeFailCaseSeriesAnalyses(
    connectionDetails = connectionDetails,
    targetDatabaseSchema = 'main',
    targetTable = 'cohort',
    dechallengeRechallengeSettings = res,
    databaseId = 'testing'
  )

  testthat::expect_true(inherits(dc, 'Andromeda'))
  testthat::expect_true(names(dc) == 'rechallengeFailCaseSeries')

  # test saving/loading
  if(actions){ # causing *** caught segfault *** address 0x68, cause 'memory not mapped' zip::zipr

  saveRechallengeFailCaseSeriesAnalyses(
    result = dc,
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_true(
    file.exists(file.path(tempdir(), 'rechallengeFailCaseSeries'))
  )

  res2 <- loadRechallengeFailCaseSeriesAnalyses(
    saveDirectory = file.path(tempdir())
  )

  testthat::expect_equal(
    nrow(dc$rechallengeFailCaseSeries %>% collect()),
    nrow(res2$rechallengeFailCaseSeries %>% collect())
  )
  }

  # check exporting to csv
  exportRechallengeFailCaseSeriesToCsv(
    result = dc,
    saveDirectory = tempdir()
  )

  countN <- dc$rechallengeFailCaseSeries %>%
    dplyr::tally()

  if(!is.null(countN$n)){
    testthat::expect_true(
      file.exists(file.path(
        tempdir(),
        'rechallenge_fail_case_series.csv'
      )
      )
    )
  }


})


test_that("computeRechallengeFailCaseSeriesAnalyses with known data", {
  tempDbLoc <- file.path(tempdir(),'db')
  if(!file.exists(tempDbLoc)){
    dir.create(tempDbLoc, recursive = T)
  }
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = 'sqlite',
    server = file.path(tempDbLoc, 'tempdb.sqlite')
    )

# check with made up date
# subject 1 has 1 exposure for 30 days
# subject 2 has 4 exposures for ~30 days with ~30 day gaps
# subject 3 has 3 exposures for ~30 days with ~30 day gaps
# subject 4 has 2 exposures for ~30 days with ~30 day gaps
targetCohort <- data.frame(
  cohort_definition_id = rep(1,10),
  subject_id = c(1,2,2,2,2,3,3,3,4,4),
  cohort_start_date = as.Date(c(
    '2001-01-01',
    '2001-01-01', '2001-03-14', '2001-05-01', '2001-07-01',
    '2001-01-01', '2001-03-01', '2001-05-01',
    '2001-01-01', '2001-03-01'
    )),
  cohort_end_date = as.Date(c(
    '2001-01-31',
    '2001-01-31', '2001-03-16', '2001-05-30', '2001-07-31',
    '2001-01-31', '2001-03-30', '2001-05-30',
    '2001-01-31', '2001-03-30'
  ))
)

# person 2 has it during 1st exposure and stops when 1st stops then restarts when 2nd starts and stops when 2nd stops
# person 3 has it during 2nd exposure and stops when 2nd stops
# person 4 has outcome whole time after 2nd exposure

outcomeCohort <- data.frame(
  cohort_definition_id = rep(2,4),
  subject_id = c(2,2,3,4),
  cohort_start_date = as.Date(c(
    '2001-01-28', '2001-03-15',
    '2001-03-01',
    '2001-03-05'
    )),
  cohort_end_date = as.Date(c(
    '2001-02-03', '2001-03-16',
    '2001-03-30',
    '2010-03-05'
    ))
)

con <- DatabaseConnector::connect(connectionDetails = connectionDetails)

DatabaseConnector::insertTable(
  data = rbind(targetCohort,outcomeCohort),
  connection = con,
  databaseSchema = 'main',
  tableName = 'cohort',
  createTable = T,
  dropTableIfExists = T,
  camelCaseToSnakeCase = F
)

res <- createDechallengeRechallengeSettings(
  targetIds = 1,
  outcomeIds = 2,
  dechallengeStopInterval = 30,
  dechallengeEvaluationWindow = 30#31
)

dc <- computeRechallengeFailCaseSeriesAnalyses(
  connectionDetails = connectionDetails,
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  dechallengeRechallengeSettings = res,
  outcomeDatabaseSchema = 'main',
  outcomeTable = 'cohort',
  databaseId = 'testing'
)

# person 2 should be in results

testthat::expect_equal(
  as.numeric(dc$rechallengeFailCaseSeries %>% dplyr::tally() %>% dplyr::collect()),
  1
)

sub <- dc$rechallengeFailCaseSeries %>% dplyr::select(.data$subjectId) %>% dplyr::collect()
testthat::expect_true(is.na(sub$subjectId))

dc <- computeRechallengeFailCaseSeriesAnalyses(
  connectionDetails = connectionDetails,
  targetDatabaseSchema = 'main',
  targetTable = 'cohort',
  dechallengeRechallengeSettings = res,
  outcomeDatabaseSchema = 'main',
  outcomeTable = 'cohort',
  databaseId = 'testing',
  showSubjectId = T
)

# person 2 should be in results

testthat::expect_equal(
  as.numeric(dc$rechallengeFailCaseSeries %>% dplyr::tally() %>% dplyr::collect()),
  1
)

sub <- dc$rechallengeFailCaseSeries %>% dplyr::select(.data$subjectId) %>% dplyr::collect()
testthat::expect_equal(sub$subjectId, 2)

})
