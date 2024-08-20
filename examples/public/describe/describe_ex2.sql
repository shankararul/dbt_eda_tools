{{
    config(
        materialized = 'view' if var('dbt_eda_tools_developer',false) else 'ephemeral'
    )
}}

{{dbt_eda_tools.describe('data_aggregated')}}
