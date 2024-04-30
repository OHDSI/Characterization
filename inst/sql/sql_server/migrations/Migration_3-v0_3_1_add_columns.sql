-- Database migrations for verion 0.3.0
-- This migration add tables

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}

-- add columns to settings
ALTER TABLE @database_schema.@table_prefixsettings ADD during_covariate_settings_json varchar(max) NULL;
ALTER TABLE @database_schema.@table_prefixsettings ADD after_covariate_settings_json varchar(max) NULL;
