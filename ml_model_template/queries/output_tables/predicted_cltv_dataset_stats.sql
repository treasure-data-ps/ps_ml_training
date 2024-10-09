WITH T0 AS (
  SELECT predicted_target as val FROM ${prediction_attr_table}
  WHERE session_id=(SELECT max(session_id) FROM ${prediction_attr_table}) 
  AND actual_target IS NOT NULL 
), 

T1 AS (
  SELECT actual_target as val FROM ${prediction_attr_table}
  WHERE session_id=(SELECT max(session_id) FROM ${prediction_attr_table}) 
  AND actual_target IS NOT NULL 
)

SELECT 
  'Original' as dataset,
  '${target_name}' as predicted_value,
  COUNT(*) as count,
  AVG(val) as mean,
  STDDEV(val) as stdev, 
  MIN(val) as min_val, 
  APPROX_PERCENTILE(val, 0.25) as q1_25,
  APPROX_PERCENTILE(val, 0.5) as q2_50,
  APPROX_PERCENTILE(val, 0.75) as q3_75,
  MAX(val) as max_val, 
  APPROX_PERCENTILE(val, 0.75)-APPROX_PERCENTILE(val, 0.25) as iqr, 
  SKEWNESS(val) as skew, 
  KURTOSIS(val) as kurt, 
  ${session_id} as session_id
FROM T1
UNION ALL 
SELECT 
  'Predicted' as dataset,
  '${target_name}' as predicted_value,
  COUNT(*) as count,
  AVG(val) as mean,
  STDDEV(val) as stdev, 
  MIN(val) as min_val, 
  APPROX_PERCENTILE(val, 0.25) as q1_25,
  APPROX_PERCENTILE(val, 0.5) as q2_50,
  APPROX_PERCENTILE(val, 0.75) as q3_75,
  MAX(val) as max_val, 
  APPROX_PERCENTILE(val, 0.75)-APPROX_PERCENTILE(val, 0.25) as iqr, 
  SKEWNESS(val) as skew, 
  KURTOSIS(val) as kurt, 
  ${session_id} as session_id
FROM T0 