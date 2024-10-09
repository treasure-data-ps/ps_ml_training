# AGG Custom Derived Attributes

This solution can serve ***most teams working with customer data*** in the CDP such as `Marketers, Data Scientists, Data Engineers, Business Decision Makers etc. The main functionality of this solution is to standerdize and automate the creation of customer attributes from existing ***beahvior*** or ***attribute*** tables in TD. The Output table can assisst with the CDP Use Cases below:

## Use Cases

- Create additional features to use for training more robust ML Models
- Add to Parent Segment to use for creating more strategic Audiences for Analysis and Activation
- Use to train Propensity (Lookalike) Models in the UI
- Activate these attributes to external systems to be consumed by customer teams who are setting up campaigns and doing analysis in external tools

# Explanation of Config Params 

The workflow is set-up, so that the end user only needs to configure the `config/input_params.yml` file in the main project folder. Details on what each parameter does below:


1. `config/input_params.yml` - Controls important parameters which are often applied globally across most of the data sources and processes in the workflow.


```YAML
#####################################################################
########################## GLOBAL PARAMS ############################
#####################################################################
sink_database: ml_prod                          #db where the final agg_attr_derived_attributes_final is written
unique_user_id: td_canonical_id                 #unique join key for all input tables
prefix: agg_attr                                #leave as Default (added to all output tables as prefix for easy find in database)
identities_table: ${prefix}_base_identities     #leave as Default --> base table of all distinct user_ids in the Parent Segment used for joining all custom feature tables to
cleanup_temp_tables: yes                        #yes - will delete all temp tables after final table is created

#####################################################################
############# TABLE PARAMS FOR SOURCE AND OUTPUT TABLES #############
#####################################################################
source_tables:                                #<--- List of JSON params to loop through and execute a custom SQL that matches the 'name' param.
  - name: base_identities
    src_db: gldn
    src_table: enriched_user_master          #this would be the source table that the custom SQL query will run on to create the derieved agg attributes from this table
    unix_timestamp: time                     #name of UNIXTIME timestamp column for behavior tables
    custom_filter:                           #allows you to apply custom filter to the SQL query by using `WHERE` notation, which getsautomatically inserted into query

  - name: pageviews
    src_db: gldn
    src_table: enrich_pageviews
    unix_timestamp: time
    custom_filter: 

  - name: email_events
    src_db: gldn
    src_table: enrich_email_events
    unix_timestamp: time
    custom_filter: 

  - name: order_events
    src_db: gldn
    src_table: enrich_orders
    unix_timestamp: time
    custom_filter: 
```
# What Tasks Are Ran by the Workflow and What are the Outputs the End User can Access

### DigDag Tasks Summary

- ***agg_custom_attributes_launch.dig*** - runs the main project workflow, that triggers entire project execution end to end, including all sub-workflows and queries in project folder.

- ***agg_custom_attributes_data_prep.dig***  - Loops through source tables and creates temp tables with custom attirbutes. Joins each temp table to base table and then renames joined temp table to the agg_attr_derived_attributes_final table. 

- ***agg_custom_attributes_cleanup.dig***  - Cleans up all temp tables from the sink database


### Table Outputs

- **agg_attr_derived_attributes_final** - the final table of all distinct user_ids as rows, and the aggregate derived attributes as columns

<img width="1023" alt="Screen Shot 2023-11-01 at 10 54 01 AM" src="https://github.com/treasure-data-ps/ps_ml_analytics_team_solutions_prod/assets/40249921/c1a6a363-595a-4d1c-b5b2-725bf17264a0">


### Additional Code Examples

#### 1. Important SQL Queries

- ***sql/pageviews.sql*** - as mentioned earlier, each SQL file name, matches the `name` parameter in the `source_tables` list in the YML. In this example below, the syntax will apply to the pageviews table and will create custom derived attributes from the pageview activity table.

```SQL
  -- Create Base input table by applying custom Filter from the params YAML and standerdizing unixtme column as tstamp, so it can be used by the code below to create time based attriutes
WITH BASE AS (
SELECT ${unique_user_id}, ${table.unix_timestamp} as tstamp, REGEXP_REPLACE(lower(TRIM(td_path)), '[^a-zA-Z0-9]', '_') as td_path
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
COUNT(*) as total_pageviews,
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

```

- ***sql/order_events.sqll*** - same as the above pageviews example, but code applies to `enriched_orders` table and creates derived attributes based on customer order activitu.



