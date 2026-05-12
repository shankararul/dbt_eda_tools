{# Create some fake missing dates at the granularity of the day #}
WITH
null_value_cols AS (
    SELECT *
    , NUll as col1_as_null
    , '' AS col2_as_empty_string
    , CASE 
        WHEN str_length = 5 THEN NULL 
        ELSE str_length 
    END AS col3_as_some_null
    FROM {{ ref('data_generator_enriched_describe') }}
)
SELECT * FROM null_value_cols
