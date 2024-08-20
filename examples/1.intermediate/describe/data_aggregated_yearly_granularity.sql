{{
    config(
        materialized = 'view' if var('dbt_eda_tools_developer',false) else 'ephemeral'
    )
}}

SELECT
    *
FROM {{ ref('data_aggregated_mixed_granularity') }}
WHERE date_mixed_granularity <= DATE('2022-01-01')
