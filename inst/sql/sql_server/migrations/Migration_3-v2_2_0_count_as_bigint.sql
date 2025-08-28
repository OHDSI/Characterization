-- Database migrations for verion 2.0.2
-- This migration updates the schema:
 -- 1. To expand the row_count column to a bigint
 -- 2. To expand the person_count column to a bigint

ALTER TABLE @database_schema.@table_prefixcohort_counts ALTER COLUMN row_count BIGINT;
ALTER TABLE @database_schema.@table_prefixcohort_counts ALTER COLUMN person_count BIGINT;
