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

- added support to enable target cohorts with multiple cohort entries for the aggregate covariate analysis by restructing to first cohort entry and ensuring the subject has a user specified minPriorObservation days observation in the database at first entry and also perform analysis on first outcomes and any outcome that is recorded during TAR.
- added shiny app


Characterization 0.0.1
======================

Initial version
