-- @TD distribute_strategy: aggressive
 WITH T1 AS (
select
table_schema || '.' || table_name as table_name
,column_name 
,data_type 
from information_schema.columns
where table_schema='${globals.sink_database}'
and table_name='cltv_test'
AND (NOT REGEXP_LIKE(column_name, '${quantitative.exclude_regexp}') AND column_name NOT IN ('${features_table_raw.date_field}', '${features_table_raw.join_key}'))
),
T2 as (
SELECT 
table_name,
column_name,
CASE 
WHEN data_type = 'varchar' THEN 'categorical'
ELSE 'numeric'
END as datatype
FROM T1
),
T3 AS (
SELECT
T2.*,
CASE
WHEN datatype = 'numeric' THEN CONCAT('rescale(', column_name, ', min(', column_name, ') over (), max(', column_name, ') over ()) as ', column_name)
ELSE column_name
END AS query_logic
FROM T2
)
SELECT array_join(array_agg(query_logic), CONCAT(', ',chr(10))) AS scale_logic
FROM T3