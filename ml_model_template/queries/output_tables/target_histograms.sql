With T0 as 
  (SELECT NUMERIC_HISTOGRAM(10, predicted_target),
    max(session_id) 
  FROM ${prediction_attr_table})

SELECT 
 session_id,
 CAST(ROUND(bin_name, 1) as VARCHAR) as label_str,
 CAST(ROUND(bin_name, 1) as DOUBLE) as label_num,
 CAST(num_vals as DOUBLE) as bin_cnt
 
 FROM T0 as x (hist, session_id)
 CROSS JOIN UNNEST(hist) as t (bin_name, num_vals)