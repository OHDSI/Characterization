% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/SaveLoad.R
\name{exportRechallengeFailCaseSeriesToCsv}
\alias{exportRechallengeFailCaseSeriesToCsv}
\title{export the RechallengeFailCaseSeries results as csv}
\usage{
exportRechallengeFailCaseSeriesToCsv(result, saveDirectory)
}
\arguments{
\item{result}{The output of running \code{computeRechallengeFailCaseSeriesAnalyses()}}

\item{saveDirectory}{An directory location to save the results into}
}
\value{
A string specifying the directory the csv results are saved to
}
\description{
export the RechallengeFailCaseSeries results as csv
}
\seealso{
Other {SaveLoad}: 
\code{\link{exportDechallengeRechallengeToCsv}()},
\code{\link{exportTimeToEventToCsv}()}
}
\concept{{SaveLoad}}
