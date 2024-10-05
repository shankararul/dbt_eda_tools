{% macro percent_of_total(column_to_aggregate, aggregation='COUNT',precision=2,level=none) %}
    {{ return(adapter.dispatch('percent_of_total', 'dbt_eda_tools')(column_to_aggregate, aggregation,precision,level)) }}
{% endmacro %}

{% macro default__percent_of_total(column_to_aggregate, aggregation,precision,level) %}

  {% set db_name = dbt_eda_tools.fetch_db() | trim %}

  {% if level %}
    {% set level_agg = "PARTITION BY " ~ level|join(',') %}
  {% endif %}

      ROUND(
        {% if aggregation|lower in ['sum','count'] %}
          {% if db_name in ('snowflake','bigquery') %}
            {{'DIV0NULL' if db_name=='snowflake' else 'SAFE_DIVIDE'}}(
              {{aggregation}}({{column_to_aggregate}})
              ,
              SUM({{aggregation}}({{column_to_aggregate}})) OVER ({{level_agg}})
            )
          {% elif db_name =='duckdb' %}
            IF(SUM({{aggregation}}({{column_to_aggregate}})) OVER ({{level_agg}}) !=0
              , {{aggregation}}({{column_to_aggregate}})
              /
              SUM({{aggregation}}({{column_to_aggregate}})) OVER ({{level_agg}})
              , NULL
            )
          {% endif %}
        {% elif aggregation|lower == 'countdistinct' %}
            {% if db_name in ('snowflake','bigquery') %}
              {{'DIV0NULL' if db_name=='snowflake' else 'SAFE_DIVIDE'}}(
                COUNT(DISTINCT {{column_to_aggregate}})
                ,
                SUM(COUNT(DISTINCT {{column_to_aggregate}})) OVER ({{level_agg}})
              )
            {% elif db_name =='duckdb' %}
              IF(SUM(COUNT(DISTINCT {{column_to_aggregate}})) OVER ({{level_agg}}) !=0
                , COUNT(DISTINCT {{column_to_aggregate}})
                /
                SUM(COUNT(DISTINCT {{column_to_aggregate}})) OVER ({{level_agg}})
                , NULL
              )
            {% endif %}
        {% else %}
          NULL
        {% endif %}
      , {{precision}})

{% endmacro %}
