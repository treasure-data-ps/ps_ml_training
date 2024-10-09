-- @TD distribute_strategy: aggressive
SELECT
${features_table_raw.join_key},
${td.last_results.scale_logic}
FROM 
  cltv_${target}