-- @TD distribute_strategy: aggressive
with features_exploded as (
  select
    t.${input_data.canonical_id},
    extract_feature(t.fv) as feature,
    extract_weight(t.fv) as value
  from
    cltv_new_data t
    LATERAL VIEW explode(features) t as fv
)
-- DIGDAG_INSERT_LINE
select
  t.${input_data.canonical_id},
  sum(m.weight * t.value) as predicted_target
from
  features_exploded t
  LEFT OUTER JOIN cltv_regressor m ON (t.feature = m.feature)
group by
  t.${input_data.canonical_id}