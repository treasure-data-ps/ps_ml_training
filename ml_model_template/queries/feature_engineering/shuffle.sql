-- @TD distribute_strategy: aggressive
SELECT T1.*,
  rand(31) as rnd
FROM cltv_base_table T1
cluster by
  rand(43)
