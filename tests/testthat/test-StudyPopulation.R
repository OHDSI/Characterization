
test_that("replaceNull", {

  testthat::expect_equal(replaceNull(0,545),0)
  testthat::expect_equal(replaceNull(NULL,545),545)

  testthat::expect_equal(replaceNull('0','545'),'0')
  testthat::expect_equal(replaceNull(NULL,'545'),'545')

})

test_that("createStudyPopulationSettings", {

  set <- createStudyPopulationSettings(targetIds = c(232,23))
  testthat::expect_true(nrow(set) == 2)

  set <- createStudyPopulationSettings(targetIds = c(232,23,232))
  testthat::expect_true(nrow(set) == 2)

  set <- createStudyPopulationSettings(targetIds = 232,
                                nestingCohortId = 12)
  testthat::expect_true(nrow(set) == 1)
  testthat::expect_true(set$targetId == 232)
  testthat::expect_true(set$nestingCohortId == 12)

  set <- createStudyPopulationSettings(targetIds = 232, minAge = 18)
  testthat::expect_true(nrow(set) == 1)
  testthat::expect_true(set$targetId == 232)
  testthat::expect_true(set$minAge == 18)

  set <- createStudyPopulationSettings(targetIds = 23, minAge = 18,
                                       genderConceptIds = c(32434,1212))
  testthat::expect_true(nrow(set) == 1)
  testthat::expect_true(set$targetId == 23)
  testthat::expect_true(set$minAge == 18)
  testthat::expect_true(set$genderConceptIds == '1212,32434')


})

test_that("combineStudyPopulationSettings", {
  set <- list(
    createStudyPopulationSettings(targetIds = c(232,23,232)),
    createStudyPopulationSettings(targetIds = 232,
                                  nestingCohortId = 12),
    createStudyPopulationSettings(targetIds = 232, minAge = 18),
    createStudyPopulationSettings(targetIds = 23, minAge = 18, genderConceptIds = c(32434,1212))
  )

  res <- combineStudyPopulationSettings(set)
  testthat::expect_true(nrow(res) == 5)
  testthat::expect_true('targetId' %in% colnames(res))
})
