WITH T1 as (
  SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = '${features_table_raw.name}' 
    AND (NOT REGEXP_LIKE(column_name, '${quantitative.exclude_regexp}') AND column_name NOT IN ('${features_table_raw.date_field}', '${features_table_raw.join_key}'))
    AND TABLE_SCHEMA = '${globals.sink_database}'
)
SELECT column_name as quant_features,
CONCAT('"', column_name, '"') AS quant_feat_quotes,
CONCAT('ln(', column_name, ' + 1)') AS quant_features_ln,
CONCAT('(CAST(', column_name, 'AS DOUBLE) - (select min(', column_name, ') from T1))/((select max(', column_name, ') from T1) - (select min(', column_name, ') from T1)) as ', column_name ) AS quant_features_minmax
FROM T1