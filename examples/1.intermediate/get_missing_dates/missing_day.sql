{{
    config(
        materialized = 'view' if var('dbt_eda_tools_developer',false) else 'ephemeral'
    )
}}

{# Create some fake missing dates at the granularity of the day #}
WITH
missing_day AS (
    SELECT *
    FROM {{ ref('data_generator') }}
    WHERE NOT (
        {# filter some specific combinations #}
        (company_name = 'FB' AND country = 'CA' AND date_day BETWEEN DATE('2019-05-01') AND DATE('2019-05-17')) OR
        (company_name = 'GOOG' AND country = 'FR' AND date_day BETWEEN DATE('2020-01-01') AND DATE('2020-05-01')) OR
        (company_name = 'AAPL' AND country = 'US' AND date_day BETWEEN DATE('2021-12-15') AND DATE('2021-12-26')) OR
        {# filter out all days across companies and countries #}
        (date_day  BETWEEN DATE('2022-05-01') AND DATE('2022-05-05')) OR
        {# filter some specific combinations for country and date #}
        (country = 'DE' AND date_day BETWEEN DATE('2019-09-07') AND DATE('2019-09-09')) OR
        {# filter some specific combinations for company and date #}
        (company_name = 'AMZN' AND date_day BETWEEN DATE('2019-09-07') AND DATE('2019-09-09'))
    )
)
SELECT * FROM missing_day
