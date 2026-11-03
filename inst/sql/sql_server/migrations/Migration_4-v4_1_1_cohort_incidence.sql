{DEFAULT @package_version = package_version}
{DEFAULT @migration = migration}
{DEFAULT @table_prefix = ''}


-- add outcome day case series columns
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates
ADD COLUMN out_day_sum_value INT;

ALTER TABLE  @database_schema.@table_prefixcase_series_covariates
ADD COLUMN out_day_average_value FLOAT;



ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_count_value INT;

ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_min_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_max_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_average_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_standard_deviation FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_median_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_p_10_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_p_25_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_p_75_value FLOAT;
ALTER TABLE  @database_schema.@table_prefixcase_series_covariates_continuous
ADD COLUMN out_day_p_90_value FLOAT;

