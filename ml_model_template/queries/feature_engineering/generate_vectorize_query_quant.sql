SELECT
      array_join(array_distinct(filter(array_agg(quant_features), x -> x IS NOT NULL)), ', ') AS quant_features,
      array_join(array_distinct(filter(array_agg(quant_feat_quotes), x -> x IS NOT NULL)), ', ') AS quant_feat_quotes
FROM schema