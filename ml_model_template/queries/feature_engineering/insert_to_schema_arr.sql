WITH T1 AS (
  SELECT '${td.each.column_name}' as column_name,
    u.src as val, 
    count(*) as cnt 
  FROM cltv_train_temp cross join
      unnest(cltv_train_temp.${td.each.column_name}) u(src)
  group by u.src
  ORDER BY 3 DESC 
  LIMIT ${input_data.one_hot_distinct_limit}

)
SELECT column_name,

CONCAT('IF(any_match(', column_name, ', e->e LIKE ', '''', val, '''', '), 1.0, 0.0) AS ', CONCAT(column_name, '_', lower(regexp_replace(val, '[\s-]', '_')))) AS transf_code,

CONCAT('IF(any_match(', column_name, ', e->e LIKE ', '''', val, '''', '), 1.0, 0.0) AS ', CONCAT(column_name, '_', lower(regexp_replace(val, '[\s-]', '_')))) AS regular_code,

'arr_one_hot' as transf_type

FROM T1

