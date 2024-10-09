WITH T1 as (
  SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = 'cltv_features_raw_${pred_table.name}'
    AND TABLE_SCHEMA = '${globals.sink_database}'
    AND column_name NOT IN ('${pred_table.join_key}', '${pred_table.date_field}')
),
T2 AS (
  SELECT column_name as quant_features,
  CONCAT('"', column_name, '"') AS quant_feat_quotes
  FROM T1
)
SELECT
      array_join(array_distinct(filter(array_agg(quant_features), x -> x IS NOT NULL)), ', ') AS quant_features,
      array_join(array_distinct(filter(array_agg(quant_feat_quotes), x -> x IS NOT NULL)), ', ') AS quant_feat_quotes
FROM T2