WITH BASE AS (
  SELECT ${unique_user_id}, ${table.unix_timestamp} as tstamp, event_type, 
  REGEXP_REPLACE(lower(TRIM(email_name)), '[^a-zA-Z0-9]', '_') as email_name, 
  REGEXP_REPLACE(lower(TRIM(campaign_name)), '[^a-zA-Z0-9]', '_') as email_campaign_name
  FROM ${table.src_db}.${table.src_table}
  ${table.custom_filter}
  )
,
AGGS AS (
  SELECT 
    ${unique_user_id},
    COALESCE(COUNT_IF(event_type='send'),0) as email_sends, 
    COALESCE(COUNT_IF(event_type='click'),0) as email_clicks,  
    COALESCE(COUNT_IF(event_type='unsubscribe'),0) as email_unsubscribes,
    COALESCE(COUNT_IF(event_type='bounce'),0) as email_bounces,
    DATE_DIFF('day', FROM_UNIXTIME(MAX(tstamp)), now() ) AS days_since_last_email_event,
    DATE_DIFF('day', FROM_UNIXTIME(MIN(tstamp)), FROM_UNIXTIME(MAX(tstamp)) ) as email_history_length,
    COALESCE(APPROX_DISTINCT(email_name),0) as distinct_emails_sent
    FROM BASE
    GROUP BY 1
  )
,
MOST AS (
  SELECT * FROM (
    SELECT 
      ${unique_user_id}, 
      COALESCE(email_campaign_name, email_name)  AS most_emailed_campaign,
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
  SELECT * FROM (
    SELECT 
      ${unique_user_id}, 
      COALESCE(email_campaign_name, email_name) AS last_email_campaign_opened,
      ROW_NUMBER() OVER (PARTITION BY ${unique_user_id} ORDER BY tstamp DESC) as rnk
    FROM 
      BASE
      WHERE event_type IN ('open', 'click')
  )
WHERE rnk = 1
  )

SELECT 
  AGGS.*, 
  MOST.most_emailed_campaign, 
  LASTE.last_email_campaign_opened
FROM AGGS
LEFT JOIN MOST ON AGGS.${unique_user_id} = MOST.${unique_user_id}
LEFT JOIN LASTE ON AGGS.${unique_user_id} = LASTE.${unique_user_id}