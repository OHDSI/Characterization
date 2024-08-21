Characterization 2.0.1
======================
- edited cohort_type in results to varchar(12)
- fixed setting id being messed up by readr loading

Characterization 2.0.0
======================
- added tests for all HADES supported dbms
- updated minCellCount censoring 
- fixed issues with incremental
- made the code more modular to enable new characterizations to be added
- added job optimization code to optimize the distributuion of jobs
- fixed tests and made minor bug updates

Characterization 1.0.0
======================
- Added parallelization in the aggregate covariates analysis
- Extact all results as csv files 
- Removed sqlite result creation
- now using ResultModelManager to upload results into database

Characterization 0.3.1
======================
- Removed DatabaseConnector from Remotes in DESCRIPTION. Fixes GitHb issue 38.
- Added check to covariateSettings input in createAggregateCovariateSettings to error if temporal is T
- adding during cohort covariate settings
- added a case covariate settings inputs to aggregate covariates
- added casePreTargetDuration and casePostTreatmentDuration integer inputs to aggregate covariates

Characterization 0.3.0
======================
- Added new outcomeWashoutDays parameter to createAggregateCovariateSettings to remove outcome occurances that are continuations of a prior outcome occurrence
- Changed the way cohort definition ids are created in createAggregateCovariateSettings to use hash of target id, outcome id and type.  This lets users combine different studies into a single result database.
- Added database migration capability and created new migrations for the recent updates.



Characterization 0.2.0
======================
Updated dependency to FeatureExtraction (>= 3.5.0) to support minCharacterizationMean paramater.


Characterization 0.1.5
======================
Changed export to csv approach to use batch export from SQLite (#41)

Characterization 0.1.4
======================
Added extra error logging

Characterization 0.1.3
======================
Optimized aggregate features to remove T and not Os (as these can be calculated using T and T and Os) - requires latest shiny app though
Optimized database extraction to csv


Characterization 0.1.2
======================
Fixing bug where first outcome was still all outcomes 
Updating shiny app to work with old and new ShinyAppBuilder

Characterization 0.1.1
======================

Fixing bug where cohort_counts were not being saved in the database 

Characterization 0.1.0
======================

- added support to enable target cohorts with multiple cohort entries for the aggregate covariate analysis by restricting to first cohort entry and ensuring the subject has a user specified minPriorObservation days observation in the database at first entry and also perform analysis on first outcomes and any outcome that is recorded during TAR.
- added shiny app


Characterization 0.0.1
======================

Initial version
