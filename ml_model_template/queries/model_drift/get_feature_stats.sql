WITH T0 AS (
  SELECT * FROM cltv_train
  UNION ALL 
  SELECT * FROM cltv_test
)

SELECT 
${session_id} as session_id,
'${td.each.column_name}' as column_name,
ROUND(AVG(${td.each.column_name}), 3) as avg_val,
ROUND(STDDEV(${td.each.column_name}), 3) as stdev_val
FROM T0