{% macro get_row_count() %}
    {{ return(adapter.dispatch('get_row_count', 'dbt_eda_tools')()) }}
{% endmacro %}

{% macro default__get_row_count() %}

    {% set sql_statement %}
        SELECT count(*)FROM {{ this }}
    {% endset %}

    {% if var('dbt_eda_tools_log_enable',false)  %}
        {% set row_count = dbt_utils.get_single_value(sql_statement) %}

        {% set headers = ['Row count::  '+this.name] %}
        {% set rows = [
            [row_count | string ]
        ] %}

        {% if headers | length and row_count != None %}
            {{ dbt_eda_tools.print_pretty_table(headers, rows) }}
        {% endif %}
    {% endif%}

{% endmacro %}
