{% macro percent_of_total(column_to_aggregate, aggregation='COUNT',precision=2,level=none) %}


{% if level %}
  {% set level_agg = "PARTITION BY " ~ level|join(',') %}
{% endif %}

    ROUND(
      {% if aggregation|lower in ['sum','count'] %}
        {{aggregation}}({{column_to_aggregate}})
        /
        SUM({{aggregation}}({{column_to_aggregate}})) OVER ({{level_agg}})
      {% elif aggregation|lower == 'countdistinct' %}
      COUNT(DISTINCT {{column_to_aggregate}})
        /
        SUM(COUNT(DISTINCT {{column_to_aggregate}})) OVER ({{level_agg}})
      {% else %}
        NULL
      {% endif %}
    , {{precision}})
{% endmacro %}
