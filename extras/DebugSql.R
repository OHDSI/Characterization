# Code for translating and rendering SQL in the package, so it can be run in a separate SQL client (e.g. SQLWorkbench)

dbms <- "redshift"
cohortDatabaseSchema <- "scratch_mschuemi"
cohortTable <- "test"

# Target-outcomes for testing ----------------------------------------------
sql <- "CREATE TABLE #exposure_outcome (target_id INT, comparator_id INT);

INSERT INTO #exposure_outcome (target_id, comparator_id)
VALUES (1, 2);"
sql <- SqlRender::translate(sql = sql, targetDialect = dbms)
SqlRender::writeSql(sql, "c:/temp/targetOutcome.sql")

# Incidence rates ----------------------------------------------------------
# Loading from relative folder, not package itself, so no need to rebuild package:
sql <- SqlRender::readSql("inst/sql/sql_server/ComputeIncidenceRates.sql")
sql <- SqlRender::render(sql = sql,
                         target_database_schema = cohortDatabaseSchema,
                         target_table = cohortTable,
                         outcome_database_schema = cohortDatabaseSchema,
                         outcome_table = cohortTable,
                         target_outcome_table = "#target_outcome",
                         risk_window_start = 0,
                         start_anchor = "cohort start",
                         risk_window_end = 0,
                         end_anchor = "cohort end")
sql <- SqlRender::translate(sql = sql, targetDialect = dbms)
SqlRender::writeSql(sql, "c:/temp/debug.sql")
