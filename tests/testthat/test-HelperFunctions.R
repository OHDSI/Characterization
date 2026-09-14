context("HelperFunctions")

test_that(".checkNotAllNull validates list contents", {
  testthat::expect_no_error(
    Characterization:::.checkNotAllNull(
      settings = list(NULL, list(a = 1), NULL)
    )
  )

  testthat::expect_error(
    Characterization:::.checkNotAllNull(
      settings = list(NULL, NULL)
    )
  )

  testthat::expect_error(
    Characterization:::.checkNotAllNull(
      settings = NULL
    )
  )
})