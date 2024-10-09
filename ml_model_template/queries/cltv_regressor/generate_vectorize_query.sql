SELECT
      array_join(array_distinct(filter(array_agg(quant_features), x -> x IS NOT NULL)), ', ') AS quant_features,
      array_join(array_distinct(filter(array_agg(quant_feat_quotes), x -> x IS NOT NULL)), ', ') AS quant_feat_quotes,
      array_join(array_distinct(filter(array_agg(quant_features_log), x -> x IS NOT NULL)), CONCAT(', ',chr(10))) AS quant_features_log,
      array_join(array_distinct(filter(array_agg(categ_features), x -> x IS NOT NULL)), ', ') AS categ_features,
      array_join(array_distinct(filter(array_agg(categ_feat_quotes), x -> x IS NOT NULL)), ', ') AS categ_feat_quotes 
FROM schema