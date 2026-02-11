SELECT cohort_definition_id/10, count(*) AS N
FROM @characterization_schema.@characterization_table
WHERE char_type = 'target'
GROUP BY cohort_definition_id, char_type;
