WITH T1 as (
  SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = 'cltv_train' 
  AND TABLE_SCHEMA = '${globals.sink_database}'
  AND NOT REGEXP_LIKE(column_name, ARRAY_JOIN(ARRAY['time', 'shuf', 'rnd', '${input_data.canonical_id}', '${input_data.target_column}', '${input_data.exclude_cols}'],'|'))
)
SELECT column_name as quant_features,
  CONCAT('"', column_name, '"') AS quant_feat_quotes
FROM T1