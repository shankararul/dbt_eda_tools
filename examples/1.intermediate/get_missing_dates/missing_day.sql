{# Create some fake missing dates at the granularity of the day #}
WITH
missing_day AS (
    SELECT *
    FROM {{ ref('data_generator') }}
    WHERE NOT (
        {# filter some specific combinations #}
        (company_name = 'FB' AND country = 'CA' AND date_day BETWEEN CAST('2019-05-01' AS DATE) AND CAST('2019-05-17' AS DATE)) OR
        (company_name = 'GOOG' AND country = 'FR' AND date_day BETWEEN CAST('2020-01-01' AS DATE) AND CAST('2020-05-01' AS DATE)) OR
        (company_name = 'AAPL' AND country = 'US' AND date_day BETWEEN CAST('2021-12-15' AS DATE) AND CAST('2021-12-26' AS DATE)) OR
        {# filter out all days across companies and countries #}
        (date_day  BETWEEN CAST('2022-05-01' AS DATE) AND CAST('2022-05-05' AS DATE)) OR
        {# filter some specific combinations for country and date #}
        (country = 'DE' AND date_day BETWEEN CAST('2019-09-07' AS DATE) AND CAST('2019-09-09' AS DATE)) OR
        {# filter some specific combinations for company and date #}
        (company_name = 'AMZN' AND date_day BETWEEN CAST('2019-09-07' AS DATE) AND CAST('2019-09-09' AS DATE))
    )
)
SELECT * FROM missing_day
