-- Database migrations for verion 2.1.1

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}

-- Change the column type of mean_exposure_time
ALTER TABLE @database_schema.@table_prefixcohort_counts ALTER COLUMN mean_exposure_time float;

