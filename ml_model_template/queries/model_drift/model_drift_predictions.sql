-- @TD distribute_strategy: aggressive
WITH T1 AS (
SELECT
time, 
session_id,
dataset,
predicted_value, 
mean,
lead(mean,1) over (partition by dataset order by time desc ) as mean_lag,
stdev,
lead(stdev,1) over (partition by dataset order by time desc ) as stdev_lag
FROM ${target_stats_table}
)
-- DIGDAG_INSERT_LINE
SELECT session_id, 
  dataset, predicted_value,
  mean, mean_lag,
  stdev, stdev_lag,
  ROUND(kld(mean, stdev, mean_lag, stdev_lag), 4) as kld_drift_coeff
FROM T1
WHERE mean IS NOT NULL and stdev IS NOT NULL 
  AND mean_lag IS NOT NULL and stdev_lag IS NOT NULL 
