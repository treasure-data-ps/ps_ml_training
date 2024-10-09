-- @TD distribute_strategy: aggressive
WITH T1 AS (
SELECT
time, 
session_id,
column_name,
avg_val,
lead(avg_val,1) over (partition by column_name order by time desc ) as mean_lag,
stdev_val,
lead(stdev_val,1) over (partition by column_name order by time desc ) as stdev_lag
FROM cltv_model_drift_features_temp
)
-- DIGDAG_INSERT_LINE
SELECT session_id, 
  column_name, avg_val, mean_lag,
  stdev_val, stdev_lag,
  ROUND(kld(avg_val, stdev_val, mean_lag, stdev_lag), 4) as kld_drift_coeff
FROM T1
WHERE avg_val IS NOT NULL and stdev_val IS NOT NULL 
  AND mean_lag IS NOT NULL and stdev_lag IS NOT NULL 