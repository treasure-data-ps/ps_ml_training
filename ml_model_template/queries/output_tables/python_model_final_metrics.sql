WITH T1 as (
  SELECT * FROM ${model_performance_table}_temp
  WHERE session_id = ${session_id}
),
T2 as (
  SELECT * FROM python_model_params
  WHERE session_id = ${session_id} 
)
SELECT T1.*,
  T2.params as hyperparams
FROM T1
JOIN T2 on T1.session_id=T2.session_id 