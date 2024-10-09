select
date_format(now(), '%Y-%m-%d %H:%i') as run_time,
${session_id} as session_id,
'test_actual${scale_target}' as source,
min(${target_name}) as min_val,
APPROX_PERCENTILE(${target_name}, 0.25) as q1,
avg(${target_name}) as avg_val,
APPROX_PERCENTILE(${target_name}, 0.5) as median,
APPROX_PERCENTILE(${target_name}, 0.75) as q3,
max(${target_name}) as max_val,
APPROX_PERCENTILE(${target_name}, 0.75) - APPROX_PERCENTILE(${target_name}, 0.25) as iqr,
STDDEV(${target_name}) as stdev,
VARIANCE(${target_name}) as var
FROM cltv_test
UNION ALL
select 
date_format(now(), '%Y-%m-%d %H:%i') as run_time,
${session_id} as session_id,
'test_predicted${scale_target}' as source,
min(predicted_target) as min_val,
APPROX_PERCENTILE(predicted_target, 0.25) as q1,
avg(predicted_target) as avg_val,
APPROX_PERCENTILE(predicted_target, 0.5) as median,
APPROX_PERCENTILE(predicted_target, 0.75) as q3,
max(predicted_target) as max_val,
APPROX_PERCENTILE(predicted_target, 0.75) - APPROX_PERCENTILE(predicted_target, 0.25) as iqr,
STDDEV(predicted_target) as stdev,
VARIANCE(predicted_target) as var
FROM cltv_predictions_test
UNION ALL
select 
date_format(now(), '%Y-%m-%d %H:%i') as run_time,
${session_id} as session_id,
'train_actual${scale_target}' as source,
min(${target_name}) as min_val,
APPROX_PERCENTILE(${target_name}, 0.25) as q1,
avg(${target_name}) as avg_val,
APPROX_PERCENTILE(${target_name}, 0.5) as median,
APPROX_PERCENTILE(${target_name}, 0.75) as q3,
max(${target_name}) as max_val,
APPROX_PERCENTILE(${target_name}, 0.75) - APPROX_PERCENTILE(${target_name}, 0.25) as iqr,
STDDEV(${target_name}) as stdev,
VARIANCE(${target_name}) as var
FROM cltv_train
UNION ALL
select 
date_format(now(), '%Y-%m-%d %H:%i') as run_time,
${session_id} as session_id,
'train_predicted${scale_target}' as source,
min(predicted_target) as min_val,
APPROX_PERCENTILE(predicted_target, 0.25) as q1,
avg(predicted_target) as avg_val,
APPROX_PERCENTILE(predicted_target, 0.5) as median,
APPROX_PERCENTILE(predicted_target, 0.75) as q3,
max(predicted_target) as max_val,
APPROX_PERCENTILE(predicted_target, 0.75) - APPROX_PERCENTILE(predicted_target, 0.25) as iqr,
STDDEV(predicted_target) as stdev,
VARIANCE(predicted_target) as var
FROM cltv_predictions_train