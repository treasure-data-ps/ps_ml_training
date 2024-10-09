-- @TD distribute_strategy: aggressive
select 
feature,
avg(weight) as weight
from 
(select 
    train_regressor(
      features, 
      ${target_name},
      '${hive.hyperparams}'
    ) as (feature, weight)
  from 
    cltv_train_vectorized
) t 
group by feature