-- @TD distribute_strategy: aggressive
SELECT
  ${input_data.canonical_id},
    quantitative_features(
      array(${td.last_results.quant_feat_quotes}),
      ${td.last_results.quant_features}
  ) as features,
  ${target_name}
FROM
  cltv_${target}