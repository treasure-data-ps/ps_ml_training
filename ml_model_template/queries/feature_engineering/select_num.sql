SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '${input_data.tbl}'
  AND TABLE_SCHEMA = '${globals.sink_database}'
  AND NOT REGEXP_LIKE(column_name, ARRAY_JOIN(ARRAY['time', 'shuf', '${input_data.canonical_id}', '${input_data.target_column}', '${input_data.exclude_cols}'],'|'))
  AND NOT REGEXP_LIKE(data_type, 'varchar')
