{{
    config(
        materialized = 'view' if env_var('DBT_ENV_CUSTOM_ENV_EDA_TOOLS_DEVELOPER',0)| int == 1 else 'ephemeral'
    )
}}

{{dbt_eda_tools.describe('data_generator_enriched_describe', include=['text','boolean','numeric'])}}
