SELECT DISTINCT array_join(flatten(array_agg(cols)), CONCAT(', ',chr(10))) AS select_cols,
array_join(flatten(array_agg(DISTINCT join_logic)), CONCAT(' ',chr(10))) AS join_logic
FROM schema
WHERE coalesce_flag = '${pred_table.coalesce}'