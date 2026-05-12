-- Asserts that string columns containing empty strings ('') are detected.
-- col2_as_empty_string is a STRING column set to '' for every row.
-- Returns rows on failure (column missing or count is zero).
WITH result AS (
    {{ dbt_eda_tools.find_null_columns('set_null_values_columns') }}
)
SELECT *
FROM result
WHERE
    column_name = 'col2_as_empty_string'
    AND null_or_empty_count = 0
UNION ALL
SELECT CAST(NULL AS STRING) AS column_name, 0 AS null_or_empty_count, 0.0 AS pct_null_or_empty, 0 AS total_rows
FROM (SELECT 1 AS _dummy)
WHERE NOT EXISTS (
    SELECT 1 FROM result WHERE column_name = 'col2_as_empty_string'
)
