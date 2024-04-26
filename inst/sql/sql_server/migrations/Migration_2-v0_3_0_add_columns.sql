-- Database migrations for verion 0.3.0
-- This migration add tables

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}

-- add columns to cohort_counts
ALTER TABLE @database_schema.@table_prefixcohort_counts ADD min_exposure_time BIGINT NULL;
ALTER TABLE @database_schema.@table_prefixcohort_counts ADD mean_exposure_time BIGINT NULL;
ALTER TABLE @database_schema.@table_prefixcohort_counts ADD max_exposure_time BIGINT NULL;

-- add columns settings
ALTER TABLE @database_schema.@table_prefixsettings ADD min_prior_observation INT NULL;
ALTER TABLE @database_schema.@table_prefixsettings ADD outcome_washout_days INT NULL;
ALTER TABLE @database_schema.@table_prefixsettings ADD min_characterization_mean FLOAT NULL;
