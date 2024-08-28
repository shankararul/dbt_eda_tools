{{
    config(
        materialized = 'view' if env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1 else 'ephemeral'
    )
}}

SELECT
    *
FROM {{ ref('data_aggregated_mixed_granularity') }}
WHERE date_mixed_granularity <= DATE('2022-01-01')
