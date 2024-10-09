WITH T1 as (
SELECT DISTINCT ${pred_table.join_key} 
FROM ${pred_table.database}.${pred_table.name}
${pred_table.filter_clause}
)
SELECT T1.*,
${td.last_results.select_cols}
FROM T1
${td.last_results.join_logic}