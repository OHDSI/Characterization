connectionDetails <- Characterization::exampleOmopConnectionDetails()

withr::defer(
  {
    unlink(file.path(tempdir(),"GiBleed.sqlite"), recursive = TRUE, force = TRUE)
  },
  testthat::teardown_env()
)
