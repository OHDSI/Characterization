Characterization
================

[![Build Status](https://github.com/OHDSI/Characterization/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/Characterization/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/Characterization/coverage.svg?branch=main)](https://codecov.io/github/OHDSI/Characterization?branch=main)


Introduction
============
Characterization is an R package for performing characterization of a target and a comparator cohort.

Features
========
- Compute incidence rates
- Compute time to event
- ...

Examples
========

```r
targetOutcomes <- data.frame(targetId = 1, comparatorId = 2)


# Connection is a DatabaseConnector connection:
result <- computeIncidenceRates(connection = connection,
                                targetDatabaseSchema = "main",
                                targetTable = "cohort",
                                comparatorDatabaseSchema = "main",
                                comparatorTable = "cohort",
                                targetOutcomes = targetOutcomes,
                                riskWindowStart = 0,
                                startAnchor = "cohort start",
                                riskWindowEnd = 0,
                                endAnchor = "cohort end")
```

Technology
============
Characterization is an R package.

System Requirements
============
Requires R (version 4.0.0 or higher). Libraries used in Characterization require Java.

Installation
=============
1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including Java.

2. In R, use the following commands to download and install Characterization:

  ```r
  install.packages("remotes")
  remotes::install_github("ohdsi/Characterization")
  ```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/Characterization).

PDF versions of the documentation are also available:
* Package manual: [Characterization.pdf](https://raw.githubusercontent.com/OHDSI/Characterization/main/extras/Characterization.pdf)

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/Characterization/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
Characterization is licensed under Apache License 2.0

Development
===========
Characterization is being developed in R Studio.

### Development status

Characterization is under development. Do not use.
