WITH
missing_values AS (
    {{
        dbt_eda_tools.get_missing_date(
            'missing_day'
            ,'date_day'
            , ['country','company_name']
            , {
                'country': ('DE','US')
                , 'company_name': ('GOG','A')
                , 'str_length': '=3'
            }
            , 'DAY'
        )
    }}

)
, row_count_missing_values AS (
    SELECT COUNT(missing_day) AS row_count
    FROM missing_values
)
-- Only rows corresponding to company A are picked up as the str_length is set to 3
SELECT * FROM row_count_missing_values WHERE row_count <> 3
