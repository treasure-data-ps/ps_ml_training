-- @TD distribute_strategy: aggressive
WITH submit as (
  select
    t.${target_name} as actual, 
    p.predicted_target as predicted
  from 
    cltv_${target} t
    JOIN cltv_predictions_${target} p 
      on (t.${input_data.canonical_id} = p.${input_data.canonical_id})
)
-- DIGDAG_INSERT_LINE
select
  ${session_id} as session_id,
  '${target}' as evaluation_table,
  ROUND(rmse(predicted, actual), 2) as RMSE,
  ROUND(mae(predicted, actual), 2) as MAE,
  ROUND(r2(predicted, actual), 2) as R2,
  ROUND(avg(actual), 2) as avg_target,
  PERCENTILE_APPROX(actual, 0.50) AS median_target,
  ROUND(STDDEV(actual), 2) as stddev_target,
  ROUND((rmse(predicted, actual) / STDDEV(actual)), 2) as rmse_stddev_ratio,
  MIN(actual) AS min_target,
  PERCENTILE_APPROX(actual, 0.25) AS Q1,
  PERCENTILE_APPROX(actual, 0.75) AS Q3,
  MAX(actual) AS max_target
  
from 
  submit