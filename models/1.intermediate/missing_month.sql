{# Create some fake missing dates at the granularity of the month #}
WITH  raw_data AS (
    SELECT
        DISTINCT
        company_name
        , country
        , CAST({{ date_trunc("month", "date_day") }} AS Date) AS date_month
    FROM {{ ref('data_generator') }}
),
missing_month AS (
    SELECT * FROM raw_data
    WHERE NOT (
        {# filter some specific combinations #}
        (company_name = 'FB' AND country = 'CA' AND date_month BETWEEN DATE('2019-05-01') AND DATE('2019-07-05')) OR
        (company_name = 'GOOG' AND country = 'FR' AND date_month BETWEEN DATE('2020-01-01') AND DATE('2020-05-01')) OR
        (company_name = 'AAPL' AND country = 'US' AND date_month BETWEEN DATE('2021-05-15') AND DATE('2021-12-26')) OR
        {# filter out all days across companies and countries #}
        (date_month  BETWEEN DATE('2022-05-01') AND DATE('2022-08-05')) OR
        {# filter some specific combinations for country and date #}
        (country = 'DE' AND date_month BETWEEN DATE('2019-09-07') AND DATE('2019-11-09')) OR
        {# filter some specific combinations for company and date #}
        (company_name = 'AMZN' AND date_month BETWEEN DATE('2019-01-07') AND DATE('2019-09-09'))
    )
)
SELECT * FROM missing_month
