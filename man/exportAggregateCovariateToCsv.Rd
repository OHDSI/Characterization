% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/SaveLoad.R
\name{exportAggregateCovariateToCsv}
\alias{exportAggregateCovariateToCsv}
\title{export the AggregateCovariate results as csv}
\usage{
exportAggregateCovariateToCsv(result, saveDirectory, minCellCount = 0)
}
\arguments{
\item{result}{The output of running \code{computeAggregateCovariateAnalyses()}}

\item{saveDirectory}{An directory location to save the results into}

\item{minCellCount}{The minimum value that will be displayed in count columns}
}
\value{
A string specifying the directory the csv results are saved to
}
\description{
export the AggregateCovariate results as csv
}
