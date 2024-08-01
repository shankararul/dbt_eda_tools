select country, count(company_name), count(distinct company_name) from {{ ref('data_aggregated') }}
group by 1
