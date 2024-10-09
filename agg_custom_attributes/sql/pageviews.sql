-- Create Base input table by applying custom Filter from the params YAML and standardizing unixtime column as tstamp, so it can be used by the code below to create time based attriutes
WITH BASE AS (
  SELECT ${unique_user_id}, ${table.unix_timestamp} as tstamp, 
    REGEXP_REPLACE(lower(TRIM(td_path)), '[^a-zA-Z0-9]', '_') as td_path
  FROM ${table.src_db}.${table.src_table}
  ${table.custom_filter}
)
--- AGGs creates aggregate attributes using GROUP BY
,
AGGS AS (
  SELECT 
    ${unique_user_id},
    DATE_DIFF('day', FROM_UNIXTIME(MAX(tstamp)), now() ) AS days_since_last_pageview,
    DATE_DIFF('day', FROM_UNIXTIME(MIN(tstamp)), FROM_UNIXTIME(MAX(tstamp)) ) as browsing_history_length,
    COALESCE(COUNT(*), 0) as total_pageviews,
    APPROX_DISTINCT(td_path) as distinct_paths_visited
  FROM BASE
  GROUP BY 1
  )
--- MOST creates aggregate attributes partitioned by count 
,
MOST AS (
  SELECT * FROM (
    SELECT 
      ${unique_user_id}, 
      td_path AS most_visited_path,
      ROW_NUMBER() OVER(PARTITION BY ${unique_user_id} ORDER BY COUNT(*) DESC) as rnk
    FROM 
      BASE
    GROUP BY 
      1, 2
    )
  WHERE rnk = 1
  )
--- LASTE creates aggregate attributes partitioned by timestamp, such as last_page_visited, 
,
LASTE AS (
  SELECT * FROM (
    SELECT 
      ${unique_user_id}, 
      td_path AS last_visited_path,
      ROW_NUMBER() OVER (PARTITION BY ${unique_user_id} ORDER BY tstamp DESC) as rnk
    FROM 
      BASE
    )
  WHERE rnk = 1
  )
--- Joins AGGS, MOST, and LAST to create the final list of derived attributes
SELECT AGGS.*, MOST.most_visited_path, LASTE.last_visited_path
FROM AGGS
LEFT JOIN MOST ON AGGS.${unique_user_id} = MOST.${unique_user_id}
LEFT JOIN LASTE ON AGGS.${unique_user_id} = LASTE.${unique_user_id}