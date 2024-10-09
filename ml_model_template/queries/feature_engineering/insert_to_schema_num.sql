-- MIN MAX SCALING --
SELECT '${td.last_results.column_name}' as column_name,
'ROUND((${td.last_results.column_name} - ${td.last_results.min_val})/(${td.last_results.max_val} - ${td.last_results.min_val}), 3) AS ${td.last_results.column_name}' AS transf_code,
'${td.last_results.column_name}' AS regular_code,
'minmax' as transf_type