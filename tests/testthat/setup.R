connectionDetails <- exampleOmopConnectionDetails()
readr::local_edition(1)
withr::defer(
  {
    unlink(file.path(tempdir(),"GiBleed.sqlite"), recursive = TRUE, force = TRUE)
  },
  testthat::teardown_env()
)

skipIfCreateTargetCohortSqlUnavailable <- function() {
  sqlAvailable <- !inherits(
    try(
      SqlRender::loadRenderTranslateSql(
        sqlFilename = "CreateTargetCohortTable.sql",
        packageName = "Characterization",
        dbms = "sqlite",
        tempEmulationSchema = "main",
        characterization_schema = "main",
        characterization_table = "char_table",
        target_attrition_table = "target_attrition",
        target_count_table = "target_count",
        case_attrition_table = "case_attrition",
        case_count_table = "case_count"
      ),
      silent = TRUE
    ),
    "try-error"
  )

  testthat::skip_if_not(
    condition = sqlAvailable,
    message = "CreateTargetCohortTable.sql not resolvable in this test context"
  )
}
