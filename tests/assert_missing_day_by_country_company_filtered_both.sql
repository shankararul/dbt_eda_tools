WITH
missing_values AS (
    {{
        get_missing_date(
            'missing_day'
            ,'date_day'
            , ['country','company_name']
            , {
                'country': ('DE')
                , 'company_name': 'A'
            }
            , 'DAY'
        )
    }}
)
, row_count_missing_values AS (
    SELECT COUNT(missing_day) AS row_count
    FROM missing_values
)
SELECT * FROM row_count_missing_values WHERE row_count <> 2
