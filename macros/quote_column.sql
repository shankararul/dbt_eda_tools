{% macro quote_column(col_name, db_name) %}
  {%- if db_name == 'snowflake' -%}
    "{{ col_name }}"
  {%- elif db_name in ('bigquery', 'duckdb') -%}
    `{{ col_name }}`
  {%- else -%}
    {{ col_name }}
  {%- endif -%}
{% endmacro %}

{% macro safe_cte_name(col_name) %}
  {%- set sanitized = col_name | replace(' ', '_') -%}
  col_{{ sanitized }}
{%- endmacro %}
