SELECT ARRAY_JOIN(ARRAY_AGG(CONCAT('AGG.', column_name)), ', ') as agg_columns
FROM INFORMATION_SCHEMA.columns
WHERE table_schema = '${sink_database}' AND table_name = '${prefix}_${table.name}' 
AND column_name not in ('time', '${unique_user_id}')