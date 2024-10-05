{# Create some fake missing dates at the granularity of the year #}
WITH
raw_data AS (
    SELECT
        DISTINCT
        company_name
        , country
        , CAST({{ date_trunc("year", "date_day") }} AS Date) AS date_year
    FROM {{ ref('data_generator') }}
)
, missing_year AS (
    SELECT * FROM raw_data
    WHERE NOT (
        {# filter some specific combinations #}
        (company_name = 'FB' AND country = 'CA' AND date_year BETWEEN CAST('2019-05-01' AS DATE) AND CAST('2020-07-05' AS DATE)) OR
        (company_name = 'GOOG' AND country = 'FR' AND date_year BETWEEN CAST('2020-01-01' AS DATE) AND CAST('2021-05-01' AS DATE)) OR
        {# filter out all days across companies and countries #}
        (date_year  BETWEEN CAST('2021-05-01' AS DATE) AND CAST('2022-08-05' AS DATE)) OR
        {# filter some specific combinations for country and date #}
        (country = 'DE' AND date_year BETWEEN CAST('2019-09-07' AS DATE) AND CAST('2020-11-09' AS DATE)) OR
        {# filter some specific combinations for company and date #}
        (company_name = 'AMZN' AND date_year BETWEEN CAST('2020-01-07' AS DATE) AND CAST('2021-09-09' AS DATE)) OR
        (date_year BETWEEN CAST('2021-05-07' AS DATE) AND CAST('2022-09-09' AS DATE))
    )
)
SELECT * FROM missing_year
