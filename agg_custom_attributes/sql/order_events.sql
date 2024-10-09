WITH BASE AS (
  SELECT ${unique_user_id}, ${table.unix_timestamp} as tstamp, order_id, order_status, order_type, coupon_used, 
    REGEXP_REPLACE(lower(TRIM(item_name)), '[^a-zA-Z0-9]', '_') as item_name, 
    REGEXP_REPLACE(lower(TRIM(item_category)), '[^a-zA-Z0-9]', '_') as item_category, 
    unit_price
  FROM ${table.src_db}.${table.src_table}
  ${table.custom_filter}
  )
,
AGGS AS (
  SELECT 
    ${unique_user_id},
    COALESCE(ROUND(SUM(unit_price), 2), 0) as total_spend,
    COALESCE(APPROX_DISTINCT(order_id), 0) as total_orders,
    COALESCE(APPROX_DISTINCT(item_name), 0) as distinct_items_ordered,
    COALESCE(APPROX_DISTINCT(item_category), 0) as distinct_categories_ordered,
    COALESCE(APPROX_DISTINCT(coupon_used), 0) as coupons_used,
    COALESCE(ROUND(SUM(unit_price) / APPROX_DISTINCT(order_id), 2), 0) AS avg_order_value,
    COALESCE(ROUND(CAST(APPROX_DISTINCT(item_name) AS DOUBLE) / APPROX_DISTINCT(order_id), 2), 0) AS avg_order_size,
    COALESCE(APPROX_DISTINCT(CASE WHEN order_type IN ('Website', 'Mobile App') THEN order_id ELSE NULL END), 0) AS online_orders, 
    COALESCE(APPROX_DISTINCT(CASE WHEN order_type IN ('In-Store') THEN order_id ELSE NULL END), 0) AS offline_orders, 
    COALESCE(APPROX_DISTINCT(CASE WHEN order_status IN ('RETURN', 'CANCEL') THEN order_id ELSE NULL END), 0) AS canceled_orders, 
    DATE_DIFF('day', FROM_UNIXTIME(MAX(tstamp)), now() ) AS days_since_last_order,
    DATE_DIFF('day', FROM_UNIXTIME(MIN(tstamp)), FROM_UNIXTIME(MAX(tstamp)) ) as order_history_length
  FROM BASE
  GROUP BY 1
  )
,
MOST AS (
  SELECT * FROM (
    SELECT 
      ${unique_user_id}, 
      item_category  AS most_ordered_category,
      ROW_NUMBER() OVER (PARTITION BY ${unique_user_id} ORDER BY COUNT(*) DESC) as rnk
    FROM 
      BASE
    GROUP BY 
      1, 2
    )
  WHERE rnk = 1
  )
,
LASTE AS (
  SELECT ${unique_user_id}, ARRAY_JOIN(ARRAY_SORT(ARRAY_DISTINCT(ARRAY_AGG(last_ordered_category))), '_') AS last_ordered_category
  FROM (
    SELECT 
      ${unique_user_id}, 
      item_category AS last_ordered_category,
      DENSE_RANK() OVER (PARTITION BY ${unique_user_id} ORDER BY tstamp DESC) as rnk
    FROM 
      BASE
    )
  WHERE rnk = 1
  GROUP BY 1
  )

SELECT AGGS.*, MOST.most_ordered_category, LASTE.last_ordered_category
FROM AGGS
LEFT JOIN MOST ON AGGS.${unique_user_id} = MOST.${unique_user_id}
LEFT JOIN LASTE ON AGGS.${unique_user_id} = LASTE.${unique_user_id}