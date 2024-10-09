SELECT T1.*, ${td.last_results.agg_columns}
FROM ${prefix}_derived_attributes_final T1 LEFT JOIN ${prefix}_${table.name} AGG 
ON T1.${unique_user_id} = AGG.${unique_user_id}