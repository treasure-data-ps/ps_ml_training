WITH T1 as (
select
  *
from
  cltv_shuffled
where
  rnd <= ${input_data.train_sample_rate}
), 

T2 AS (
  SELECT tt.*,
    (SELECT min(${tgt}) FROM T1) as ${tgt}_min,
    (SELECT max(${tgt}) FROM T1) as ${tgt}_max,
    (SELECT ROUND(avg(${tgt}), 3) FROM T1) as ${tgt}_avg,
    (SELECT ROUND(stddev(${tgt}), 3) FROM T1) as ${tgt}_stdev,
    (SELECT ROUND(APPROX_PERCENTILE(${tgt}, 0.25), 3) FROM T1) as ${tgt}_q1,
    (SELECT ROUND(APPROX_PERCENTILE(${tgt}, 0.5), 3) FROM T1) as ${tgt}_median,
    (SELECT ROUND(APPROX_PERCENTILE(${tgt}, 0.75), 3) FROM T1) as ${tgt}_q3
  FROM (
    SELECT *
    FROM cltv_shuffled
    WHERE rnd > ${input_data.train_sample_rate}
    ) tt -- scaling based on training data 
)

SELECT T2.*, 
ROUND((${tgt} - ${tgt}_min) / (${tgt}_max - ${tgt}_min), 5) as ${tgt}_minmax,
ROUND((${tgt} - ${tgt}_avg) / ${tgt}_stdev, 5) AS ${tgt}_zscore,
ROUND(if(is_nan(ln(${tgt} + 1)), 0.00, ln(${tgt} + 1)), 5) as ${tgt}_ln,
ROUND((${tgt} - ${tgt}_median) / (${tgt}_q3 - ${tgt}_q1), 5) as ${tgt}_robust
FROM T2