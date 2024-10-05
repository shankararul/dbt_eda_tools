WITH
percent_of_total AS (
    SELECT
        company_name
        , country
        , {{dbt_eda_tools.percent_of_total('str_length',precision=3)}} AS count_percent
        , {{dbt_eda_tools.percent_of_total('str_length',precision=3, level=['company_name'])}} AS count_percent_level

    FROM {{ ref('data_aggregated') }}
    GROUP BY 1,2
    ORDER BY company_name, country, sum(str_length)  DESC
)
SELECT * FROM percent_of_total
WHERE
    1=1
    -- aggregated percent at level is always greater than percent of entire column
    AND
    (
        count_percent_level < count_percent
    -- MSFT has 3 countries, so each should be 1/3 of the total
    OR (company_name = 'MSFT' AND count_percent_level <> .333)
    -- AMZN & FB are unfiltered and have 5 countries, so each should be 1/5 of the total
    OR (company_name IN ('AMZN','FB') AND count_percent_level <> .2)
    -- Each with 4 countries, so each should be 1/4 of the total
    OR (company_name IN ('GOG','A') AND count_percent_level <> .25)
    )
