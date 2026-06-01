{DEFAULT @drop_char_cohorts = true}
{DEFAULT @drop_char_counts = true}
{DEFAULT @drop_char_attr = true}
{DEFAULT @drop_char_settings = true}

{@drop_char_cohorts}?{
DROP TABLE IF EXISTS @characterization_schema.@characterization_table;
}

{@drop_char_counts}?{
DROP TABLE IF EXISTS @characterization_schema.@target_count_table;
DROP TABLE IF EXISTS @characterization_schema.@case_count_table;
}

{@drop_char_attr}?{
DROP TABLE IF EXISTS @characterization_schema.@target_attrition_table;
DROP TABLE IF EXISTS @characterization_schema.@case_attrition_table;
}

{@drop_char_settings}?{
DROP TABLE IF EXISTS @characterization_schema.@target_settings_table;
DROP TABLE IF EXISTS @characterization_schema.@case_settings_table;
}

