-- @TD distribute_strategy: aggressive
SELECT
  ${pred_table.join_key},
    quantitative_features(
      ARRAY(${td.last_results.quant_feat_quotes}),
      ${td.last_results.quant_features}
  ) AS features
FROM
  cltv_features_raw_${pred_table.name}