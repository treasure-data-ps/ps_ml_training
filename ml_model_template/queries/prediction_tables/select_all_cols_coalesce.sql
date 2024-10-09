WITH T1 as (
  SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = '${join_table.name}' 
    AND TABLE_SCHEMA = '${globals.sink_database}'
    AND column_name NOT IN ('${join_table.date_field}', '${join_table.join_key}')
    ${join_table.exclude_cols}
    ${join_table.select_cols_syntax}
),
T2 as (
SELECT
  CONCAT('CAST(COALESCE(', column_name, ', ${join_table.coal_nums}) AS DOUBLE) AS ', column_name) AS col,
  '${prediction_tables.join_type} JOIN ${join_table.database}.${join_table.name} ${join_table.name} ON T1.${globals.canonical_id} = ${join_table.name}.${join_table.join_key}' AS join_logic,
  '${join_table.name}' AS tbl_name
FROM T1
)
SELECT
array_distinct(array_agg(tbl_name)) as tbl_names,
'all' as cols_type,
'yes' as coalesce_flag,
array_distinct(array_agg(join_logic)) as join_logic,
array_distinct(array_agg(col)) as cols
FROM T2