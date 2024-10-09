WITH T0 as (
  SELECT distinct session_id, 
    TD_TIME_FORMAT(time, 'yyyy-MM-dd')  as rundate
  FROM ${model_performance_table}
)

SELECT rundate, session_id FROM T0