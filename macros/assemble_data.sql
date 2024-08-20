{% macro assemble_data(tables, columns, output_name, db_name) %}
    {{ return(adapter.dispatch('assemble_data', 'dbt_eda_tools')(tables, columns, output_name, db_name)) }}
{% endmacro %}

{% macro default__assemble_data(tables, columns, output_name, db_name) %}
    {# cannot use dbt_utils.union_relations because it does not support CTEs in macro #}
    {{output_name}} AS (
        {% for tbl in tables %}
            SELECT
                {% for col in columns%}
                    {% if not loop.first %},{% endif %} {{col}}
                {% endfor %}
            FROM
            {{tbl}}
            {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    )

{% endmacro %}
