WITH T0 as (
  SELECT '${session_id}' as session_id, 
    ARBITRARY(TD_TIME_FORMAT(time, 'yyyy-MM-dd')) as rundate,
    '${target_name}' as target,
    count(${input_data.canonical_id}) as train_size 
  FROM cltv_train 
), 

T1 as (
  SELECT '${session_id}' as session_id, 
    count(${input_data.canonical_id}) as test_size 
  FROM cltv_test
), 

T2 as (
  SELECT '${session_id}' as session_id, 
    COUNT(column_name) as num_features

  FROM INFORMATION_SCHEMA.COLUMNS 
  WHERE table_name='cltv_train'
  AND TABLE_SCHEMA = '${globals.sink_database}'
and NOT REGEXP_LIKE(column_name, '${input_data.canonical_id}|time|${target_name}')
)

SELECT ${session_id} as session_id, 
  T0.rundate as rundate, 
  T0.target as target, 
  T0.train_size as train_size, 
  T1.test_size as test_size, 
  T2.num_features as features_count, 
  'linreg' as model_type, 
  '${hive.hyperparams}' as model_params, 
  '${input_data.scale_features}' as scale_features
FROM T0 
JOIN T1 on T0.session_id=T1.session_id
JOIN T2 on T0.session_id=T2.session_id