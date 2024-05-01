-- Database migrations for verion 0.3.0
-- This migration add tables

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}

-- add columns to settings
ALTER TABLE @database_schema.@table_prefixsettings ADD case_covariate_settings_json varchar(max) NULL;
ALTER TABLE @database_schema.@table_prefixsettings ADD case_post_outcome_duration INT NULL;
ALTER TABLE @database_schema.@table_prefixsettings ADD case_pre_target_duration INT NULL;
