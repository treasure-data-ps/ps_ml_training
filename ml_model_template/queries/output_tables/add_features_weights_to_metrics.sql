WITH T1 AS (
select * from cltv_regressor 
order by weight desc
),
T2 as (
SELECT
${session_id} as session_id,
array_agg(feature) as features, 
array_agg(round(weight, 4)) as weights
from T1
),
T3 as (
SELECT * FROM ${model_performance_table}_temp
WHERE session_id = ${session_id}
)
SELECT T3.*, 
T2.features, 
T2.weights
FROM T3
LEFT JOIN T2
ON T3.session_id = T2.session_id
