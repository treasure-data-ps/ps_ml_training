WITH T1 as (
  SELECT ${input_data.canonical_id}, 
    '${target_name}' as target,
    predicted_target
  FROM cltv_predictions_test
  UNION ALL
  SELECT ${input_data.canonical_id}, 
    '${target_name}' as target,
    predicted_target
  FROM cltv_predictions_train
), 
T2 as (
  SELECT ${input_data.canonical_id}, 
    ${target_name} as actual_target
  FROM cltv_train 
  UNION ALL 
  SELECT ${input_data.canonical_id}, 
    ${target_name} as actual_target
  FROM cltv_test 
)
SELECT 
  ${session_id} as session_id,
  T1.${input_data.canonical_id}, 
  '${target_name}' as target,
  predicted_target,
  T2.actual_target,
CASE 
  WHEN predicted_target >= (select AVG(${target_name}) from cltv_train) THEN 'Above AVG'
  ELSE 'Below AVG'
  END as cltv_prediction_category
FROM T1
JOIN T2 
  ON T2.${input_data.canonical_id}=T1.${input_data.canonical_id}