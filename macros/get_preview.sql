{% macro get_preview(nbr_rows=5) %}
    {{ return(adapter.dispatch('get_preview', 'dbt_eda_tools')(nbr_rows)) }}
{% endmacro %}

{% macro default__get_preview(nbr_rows) %}

    {% set sql_statement %}
        SELECT * FROM {{ this }} LIMIT {{nbr_rows}}
    {% endset %}

    {% if var('dbt_eda_tools_log_enable',false)  %}
        {% set head_rows = dbt_utils.get_query_results_as_dict(sql_statement) %}

        {% set rows = head_rows.values() | list %}
        {% set total_columns = rows | length %}
        {% set total_length_field = 16 %}
        {% set nbr_of_columns = 7 %}

        {% set headers = [] %}
        {% for item in head_rows.items() %}
            {% set field_value = item[0] | string %}
            {% set formatted_item = "{}".format(field_value if field_value|length <total_length_field else field_value[:total_length_field]+".." ) %}
            {% if total_columns > nbr_of_columns %}
                {%- if loop.index<=nbr_of_columns-2 and headers.insert(loop.index,formatted_item) %}{% endif -%}
            {% else %}
                {%- if total_columns <=nbr_of_columns and headers.insert(loop.index,formatted_item) %}{% endif -%}
            {% endif -%}
        {% endfor %}

        {%- if total_columns > nbr_of_columns and headers.insert(nbr_of_columns-2,'..') %}{% endif -%}

        {% if total_columns > nbr_of_columns and head_rows.items()|list|length %}
            {% set last_value = (head_rows.items()|list)[-2][0] | string %}
            {% set formatted_item = "{}".format(last_value if last_value|length <total_length_field else last_value[:total_length_field]+".." ) %}
            {%- if headers.insert(nbr_of_columns,formatted_item) %}{% endif -%}

            {% set last_value = (head_rows.items()|list)[-1][0] | string %}
            {% set formatted_item = "{}".format(last_value if last_value|length <total_length_field else last_value[:total_length_field]+".." ) %}
            {%- if headers.insert(nbr_of_columns,formatted_item) %}{% endif -%}

        {% endif %}



        {% if total_columns > nbr_of_columns %}
            {% set rows = rows[:nbr_of_columns-2]+rows[-2:] %}
        {% else %}
            {% set rows = rows[:nbr_of_columns] %}
        {% endif %}

        {% set nbr_of_lines = rows[0] | length %}
        {% set formatted_row_list = [] %}

        {% for cntr in range(nbr_of_lines) %}
            {% set formatted_rows = [] %}

            {% for item in rows %}
                {% set field_value = item[cntr] | string %}
                {% set formatted_item = "{}".format(field_value if field_value|length <total_length_field else field_value[:total_length_field]+".." ) %}
                {%- if formatted_rows.insert(loop.index,formatted_item) %}{% endif -%}

            {% endfor %}
            {%- if total_columns > nbr_of_columns and formatted_rows.insert(nbr_of_columns-2,'..') %}{% endif -%}

            {%- if formatted_row_list.insert(cntr,formatted_rows|list) %}{% endif -%}
        {% endfor %}

        {% if headers | length %}
            {{ log("\033[1;32m " + this.name+ " preview:\033[0m", info=True) }}
            {{ dbt_eda_tools.print_pretty_table(headers, formatted_row_list) }}
        {% endif %}
    {% endif%}

{% endmacro %}
