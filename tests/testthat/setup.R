connectionDetails <- Characterization::exampleOmopConnectionDetails()
readr::local_edition(1)
withr::defer(
  {
    unlink(file.path(tempdir(),"GiBleed.sqlite"), recursive = TRUE, force = TRUE)
  },
  testthat::teardown_env()
)
