WITH T1 as (
  SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = '${features_table_raw.name}' 
    AND (REGEXP_LIKE(column_name, '${categorical.include_regexp}') AND column_name NOT IN ('${features_table_raw.date_field}', '${features_table_raw.join_key}'))
    AND TABLE_SCHEMA = '${globals.sink_database}'
)
SELECT column_name as categ_features,
CONCAT('"', column_name, '"') AS categ_feat_quotes
FROM T1