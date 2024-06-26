% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/RunCharacterization.R
\name{runCharacterizationAnalyses}
\alias{runCharacterizationAnalyses}
\title{execute a large-scale characterization study}
\usage{
runCharacterizationAnalyses(
  connectionDetails,
  targetDatabaseSchema,
  targetTable,
  outcomeDatabaseSchema,
  outcomeTable,
  tempEmulationSchema = NULL,
  cdmDatabaseSchema,
  characterizationSettings,
  saveDirectory,
  tablePrefix = "c_",
  databaseId = "1",
  showSubjectId = F,
  minCellCount = 0
)
}
\arguments{
\item{connectionDetails}{The connection details to the database containing the OMOP CDM data}

\item{targetDatabaseSchema}{Schema name where your target cohort table resides. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{targetTable}{Name of the target cohort table.}

\item{outcomeDatabaseSchema}{Schema name where your outcome cohort table resides. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{outcomeTable}{Name of the outcome cohort table.}

\item{tempEmulationSchema}{Some database platforms like Oracle and Impala do not truly support temp tables.
To emulate temp tables, provide a schema with write privileges where temp tables
can be created}

\item{cdmDatabaseSchema}{The schema with the OMOP CDM data}

\item{characterizationSettings}{The study settings created using \code{createCharacterizationSettings}}

\item{saveDirectory}{The location to save the results to}

\item{tablePrefix}{A string to append the tables in the results}

\item{databaseId}{The unique identifier for the cdm database}

\item{showSubjectId}{Whether to include subjectId of failed rechallenge case series or hide}

\item{minCellCount}{The minimum count value that is calculated}
}
\value{
An sqlite database with the results is saved into the saveDirectory and a csv file named tacker.csv
details which analyses have run to completion.
}
\description{
Specify the database connection containing the CDM data, the cohort database schemas/tables,
the characterization settings and the directory to save the results to
}
\details{
The results of the characterization will be saved into an sqlite database inside the
specified saveDirectory
}
