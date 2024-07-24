{#

-- Examples of how the macro could be potentially invoked

{{get_missing_date('missing_day','date_day', [], {}, 'DAY')}} --1 row
{{get_missing_date('missing_month','date_month', [], {}, 'MONTH')}} --1 row
{{get_missing_date('missing_month','date_month', [], {}, 'DAY')}} --67 rows
{{get_missing_date('missing_year','date_year', [], {}, 'YEAR')}} --1 row
{{get_missing_date('missing_year','date_year', [], {}, 'MONTH')}} --4 rows
{{get_missing_date('missing_day','date_day', ['company_name'], {}, 'DAY')}} --6 rows
{{get_missing_date('missing_day','date_day', ['country'], {}, 'DAY')}} --6 rows
{{get_missing_date('missing_day','date_day', ['country','company_name'], {}, 'DAY')}} --35 rows

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
}} -- 2 rows
#}

{{
    get_missing_date(
        'missing_month'
        ,'date_month'
        , ['country','company_name']
        , {
            'country': ('DE')
        }
        , 'MONTH'
    )
}}
