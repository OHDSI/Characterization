-- Database migrations for verion 0.3.0
-- This migration add tables

{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}

-- add columns to settings
CREATE TABLE @database_schema.@table_prefixtime_at_risk(
run_id INT,
database_id VARCHAR(100),
time_at_risk_id INT,
risk_window_start INT,
risk_window_end INT,
start_anchor VARCHAR(15)
end_anchor VARCHAR(15)
);

ALTER TABLE @database_schema.@table_prefixcohort_details ADD time_at_risk_id INT NULL;

ALTER TABLE @database_schema.@table_prefixsettings DROP risk_window_start;
ALTER TABLE @database_schema.@table_prefixsettings DROP risk_window_end;
ALTER TABLE @database_schema.@table_prefixsettings DROP start_anchor;
ALTER TABLE @database_schema.@table_prefixsettings DROP end_anchor;
