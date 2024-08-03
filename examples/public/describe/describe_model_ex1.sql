
select count(*) as columns,
      count(distinct t.table_schema || '.' ||  t.table_name) as tables
  from information_schema.tables t
  inner join information_schema.columns c on
         c.table_schema = t.table_schema and c.table_name = t.table_name
  where t.table_type = 'BASE VIEW'
