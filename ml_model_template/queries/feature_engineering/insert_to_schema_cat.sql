WITH T1 AS (
  SELECT '${td.each.column_name}' as column_name, 
    ${td.each.column_name} as val, 
    count(*) as cnt
  FROM cltv_train_temp
  GROUP BY 1, 2
  ORDER BY 3 desc
  LIMIT ${input_data.one_hot_distinct_limit}
)
SELECT column_name,
CONCAT('IF(', column_name, ' = ', '''', val, '''', ', 1.0, 0.0) AS ', CONCAT(column_name, '_', lower(regexp_replace(val, '[\s-]', '_')))) AS transf_code,
CONCAT('IF(', column_name, ' = ', '''', val, '''', ', 1.0, 0.0) AS ', CONCAT(column_name, '_', lower(regexp_replace(val, '[\s-]', '_')))) AS regular_code,
'one_hot' as transf_type
FROM T1 