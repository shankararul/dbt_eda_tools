WITH date_gen AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2025-01-01' as date)"
   )
}}
),
company_gen AS (
     SELECT 'MSFT' AS company_name
     UNION ALL
     SELECT 'GOG' AS company_name
     UNION ALL
     SELECT 'AMZN' AS company_name
     UNION ALL
     SELECT 'A' AS company_name
     UNION ALL
     SELECT 'FB' AS company_name
),
country_gen AS (
    SELECT 'FR' AS country
    UNION ALL
    SELECT 'DE' AS country
    UNION ALL
    SELECT 'GB' AS country
    UNION ALL
    SELECT 'US' AS country
    UNION all
    SELECT 'CA' AS country
)
{# Generate all combos of date, company, and countrys between 2019-01-01 and 2025-01-01 #}
SELECT *, LENGTH(company_name)+LENGTH(country) AS str_length
FROM date_gen, company_gen, country_gen
