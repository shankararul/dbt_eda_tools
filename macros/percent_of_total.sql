{% macro percent_of_total(column_to_aggregate, aggregation='COUNT',precision=2,level=none) %}

{% set db_name = fetch_db() | trim %}

{% if level %}
  {% set level_agg = "PARTITION BY " ~ level|join(',') %}
{% endif %}

    ROUND(
      {% if aggregation|lower in ['sum','count'] %}
        {{'DIV0NULL' if db_name=='snowflake' else 'SAFE_DIVIDE'}}(
          {{aggregation}}({{column_to_aggregate}})
          ,
          SUM({{aggregation}}({{column_to_aggregate}})) OVER ({{level_agg}})
        )
      {% elif aggregation|lower == 'countdistinct' %}
          {{'DIV0NULL' if db_name=='snowflake' else 'SAFE_DIVIDE'}}(
            COUNT(DISTINCT {{column_to_aggregate}})
            ,
            SUM(COUNT(DISTINCT {{column_to_aggregate}})) OVER ({{level_agg}})
          )
      {% else %}
        NULL
      {% endif %}
    , {{precision}})
{% endmacro %}
