{% macro print_pretty_table(headers,rows) %}
    {{ return(adapter.dispatch('print_pretty_table', 'dbt_eda_tools')(headers,rows)) }}
{% endmacro %}

{% macro ljust(value, width, type) %}
    {{ return(adapter.dispatch('ljust', 'dbt_eda_tools')(value, width, type)) }}
{% endmacro %}

{% macro print_delimiter(delimiter_sign, col_widths_max) %}
    {{ return(adapter.dispatch('print_delimiter', 'dbt_eda_tools')(delimiter_sign, col_widths_max)) }}
{% endmacro %}

{% macro default__ljust(value, width, type) %}
    {{ ("\033[32m " if type=='val' else "\033[1;32m ") + value + (' ' * (width - value | length))+" \033[0m" }}
{% endmacro %}

{% macro default__print_delimiter(delimiter_sign, col_widths_max) %}
    {%- set delimiter = [] -%}

    {%- for item in col_widths_max -%}
        {%- for i in range(item) -%}
            {%- do delimiter.append(delimiter_sign) -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- do return(delimiter) -%}
{% endmacro %}

{% macro default__print_pretty_table(headers, rows) %}

    {%- set col_widths_max = [] -%}
    {%- set col_widths = [] -%}
    {%- for i in range(headers | length) -%}
        {%- set col_widths = [] -%}
        {%- if col_widths.insert(i,headers[i] | length) %}{% endif -%}
        {%- for row in rows -%}
            {%- if col_widths.insert(i,row[i] | length) %}{% endif -%}
        {%- endfor -%}
        {%- if col_widths_max.insert(i,col_widths|max) %}{% endif -%}
    {%- endfor -%}




    {%- set table = [] -%}
    {%- do table.append(' '.join([])) -%}

    {%- set delimiter_line = dbt_eda_tools.print_delimiter("-", col_widths_max) -%}
    {%- do table.append('-'.join(delimiter_line)) -%}


    {%- set header_row = [] -%}
    {%- for i in range(headers | length) -%}
        {%- set header = dbt_eda_tools.ljust(headers[i], col_widths_max[i],'header') -%}
        {%- do header_row.append(header| replace('\n', '')) -%}
    {%- endfor -%}

    {%- do table.append(' - '.join([])) -%}
    {%- do table.append(' | '.join(header_row)) -%}

    {%- set delimiter_line = dbt_eda_tools.print_delimiter("-", col_widths_max) -%}
    {%- do table.append('-'.join(delimiter_line)) -%}

    {%- for row in rows -%}
        {%- set row_data = [] -%}
        {%- for i in range(row | length) -%}
            {%- set cell = dbt_eda_tools.ljust(row[i], col_widths_max[i], 'val') -%}
            {%- do row_data.append(cell | replace('\n', '')) -%}
        {%- endfor -%}
        {%- do table.append(' | '.join(row_data)) -%}
    {%- endfor -%}
    {%- do table.append(' ') -%}

    {{ log(table | join('\n'), info=True) }}
{% endmacro %}
