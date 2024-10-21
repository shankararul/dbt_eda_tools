{% macro get_missing_date(model_name, date_col, dimensions, filters, expected_frequency) %}
    -- depends_on: {{ ref(model_name) }}
    {% if execute and load_relation(ref(model_name)) %}
      {{ return(adapter.dispatch('get_missing_date', 'dbt_eda_tools')(model_name, date_col, dimensions, filters, expected_frequency)) }}
    {% endif %}
{% endmacro %}

{% macro default__get_missing_date(model_name, date_col, dimensions, filters, expected_frequency) %}
  WITH
  unique_dates AS (
    SELECT
      DISTINCT
          {{date_col}}
          {% for col in dimensions %}
              , ({{ col }}) AS {{col}}
          {% endfor %}
    FROM {{ ref(model_name) }}
    {% if filters %}
      {% for key, value in filters.items() %}
          {% if loop.index == 1 %}
            WHERE 1=1
          {% endif %}

          {# if the argument is not a tuple for which we need to use an 'IN' operator #}
          {% if value is string %}
            {%- set numeric_check = (value.replace("<", "").replace(">", "").replace("=", "")) -%}

            {# if numeric value and contains an arithmetric comparison operator #}
            {% if numeric_check|int != 0 and modules.re.match('<|>|=',value) %}
              AND  {{ key }} {{value}}
            {% else %}
              AND  {{ key }} = '{{ value }}'
            {% endif %}

          {% else %}
            AND {{ key }} IN {{ value }}
          {% endif %}
      {% endfor %}
    {% endif %}
  )
  , dates_lagged AS (
    SELECT *
    , LAG({{date_col}},1) OVER (
          ORDER BY
          {% for col in dimensions %}
              ({{ col }}) DESC,
          {% endfor %}
          {{date_col}} DESC) AS next_{{date_col|lower}}
    FROM unique_dates
  )
  SELECT
      {{date_col}}
      {% for col in dimensions %}
          , ({{ col }}) AS {{col}}
      {% endfor %}
      , next_{{date_col}}
      , ABS({{datediff('next_'+date_col,date_col,expected_frequency)}}) AS missing_{{expected_frequency|lower}}
  FROM dates_lagged
  WHERE {{datediff('next_'+date_col,date_col,expected_frequency)}} < -1
{% endmacro %}
