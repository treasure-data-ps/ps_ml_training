SELECT
  ${session_id} as session_id,
  cltv_prediction_category,
  COUNT(*) as cltv_counts
FROM ${prediction_attr_table}
GROUP BY 1, 2


