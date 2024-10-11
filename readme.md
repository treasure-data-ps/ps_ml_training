# Building a Custom ML Workflow: Best Practices

This page walks through the processes and best practices for creating a custom Machine Learning workflow in Treasure Data.  
[GitHub link to template](https://github.com/treasure-data-ps/ps_ml_training)

## Overall Best Practices

These are some things to consider before you begin writing your workflow, and while the workflow is being built:

- **Input parameters** – What variables of the workflow would be more optimal as a parameter? Examples include output database, modeling hyperparameters, data processing options (scaling, one-hot encoding), etc.
  
- **Output table naming** – It is highly recommended that all tables output from a single project have the same prefix. This makes it easier to search for relevant tables in databases and also simplifies cleanup. Intermediate tables (not used as final outputs) should have a `_temp` suffix.
  
- **Appending vs. overwriting** – Decide what information from the ML process you want to track (model parameters and metrics, statistics on results) vs. what you don’t need to store (e.g., full customer results).
  
- **Session ID** – Useful for tracking runs and debugging, especially with tables that have multiple model runs. You can use this variable in DigDag/Presto/Hive by calling `${session_id}`.

## Data Preparation

### Aggregating Customer Behaviors

Most predictive models require customer data in an attribute (one-to-one) format, with one row per customer ID. Often, data on transactions, orders, emails, and web activity come in a behavior format, which isn’t viable for modeling. To solve this, we can use an additional workflow project `agg_custom_attributes` to create attributes from multiple behavior tables.

This workflow aggregates behaviors into a single “custom attributes” table. The YML file allows us to list all the source behavior tables, as seen below.

```yaml
#####################################################################
########################## GLOBAL PARAMS ############################
#####################################################################
sink_database: ml_training_cdp_world_2024       # db where output tables will be written
unique_user_id: td_canonical_id                 # unique join key for all input tables
prefix: agg_attr                                # added to all output tables as prefix for easy find in database
identities_table: enriched_user_master          # base table of all distinct user_ids in the Parent Segment 
cleanup_temp_tables: yes                        # yes/no - delete all temp tables after final table is created

#####################################################################
############# TABLE PARAMS FOR SOURCE AND OUTPUT TABLES #############
#####################################################################

source_tables: 
  - name: order_events
    src_db: gldn
    src_table: orders       # source table to create derived agg attributes from 
    unix_timestamp: time    # name of UNIXTIME timestamp column for behavior table
    custom_filter:          # `WHERE` clause, to filter behavior table
    
  - name: pageviews
    src_db: gldn
    src_table: pageviews
    unix_timestamp: time
    custom_filter: 
```

The workflow loops through each of the source tables, runs an aggregation query for each, and joins all aggregated attributes into one output table. This output table can then be joined with other attribute tables for modeling or added to a Parent Segment for use in Audience Studio.

## Feature Engineering

### Train-test Split

To prevent data leakage, train-test splitting should occur before any other data preparation steps. Data can be split randomly (e.g., 80% train, 20% test) or defined by the model use-case (e.g., by time or customer segment).

To perform a random split, we can use Hive to create a column of random values between 0-1, as shown below:

```sql
-- @TD distribute_strategy: aggressive
SELECT T1.*,
  rand(31) as rnd
FROM cltv_base_table T1
CLUSTER BY rand(43)
```

### Outlier Imputation

For datasets with many outliers, imputation can be considered. Below is an example using the Interquartile Range (IQR) method on a single column. The `outlier_ratio` is a parameter (usually set between 1-2).

```sql
WITH iqr AS (
  SELECT (APPROX_PERCENTILE(monetary, 0.75) - 
      APPROX_PERCENTILE(monetary, 0.25))*${outlier_ratio} as outlier_threshold,
      APPROX_PERCENTILE(monetary, 0.5) as median
  FROM cltv_base_table)
  
SELECT CASE
  WHEN monetary > (SELECT median+outlier_threshold FROM iqr) THEN 
    (SELECT median+outlier_threshold FROM iqr)
  WHEN monetary < (SELECT median-outlier_threshold FROM iqr) THEN 
    (SELECT median-outlier_threshold FROM iqr)
  ELSE monetary END as monetary
FROM cltv_base_table
```

### One-hot Encoding Categorical/Array Variables

It is good practice to enforce a one-hot encoding limit to prevent an explosion of columns when dealing with many unique values. Below is an example using DigDag:

```yaml
+select_categorical_columns:
  database: ${input_data.db}
  td_for_each>: queries/feature_engineering/select_categorical.sql
  _parallel: true
  _do:
    +check_if_feature_valid:
      td>:
      query: "SELECT COUNT(DISTINCT(${td.each.column_name})) as nu FROM cltv_train_temp"
      store_last_results: true 
      
    +insert_onehot_syntax:
      if>: ${td.last_results.nu > 1}
      _do:
        +insert:
          td>: queries/feature_engineering/insert_to_schema_cat.sql
          insert_into: schema
```

### Scaling Numerical Features

For scaling, here is an example of MinMax scaling. The minimum and maximum values are checked, and if they are equal, the feature is excluded from modeling.

```yaml
+numerical_features:
  +select_num_columns:
    database: ${input_data.db}
    td_for_each>: queries/feature_engineering/select_num.sql
    _parallel: true
    _do: 
      +get_num_agg:
        td>:
        query: "SELECT '${td.each.column_name}' as column_name, CAST(min(${td.each.column_name}) as varchar) as min_val, CAST(max(${td.each.column_name}) as varchar) as max_val FROM cltv_train_temp"
        store_last_results: true
      +check_variance:
        if>: ${td.last_results.min_val != td.last_results.max_val}
        _do:
          +insert_num_syntax:
            td>: queries/feature_engineering/insert_to_schema_num.sql
            insert_into: schema
```

## Model Training

For more information on model training, see the [Hive Documentation](https://hivemall.github.io/).

### Training Python Models

To use Python models, remember to set your API key in the project's secrets tab. Here are some useful functions:

To import tables from TD to Python:

```python
def get_table(table, db, apikey, td_api_server):
    """Retrieve table from TD account."""
    with tdclient.Client(apikey=apikey, endpoint=td_api_server) as td:
        job = td.query(db, f"SELECT * FROM {table}", type='presto')
        job.wait()
        data = job.result()
        columns = [f[0] for f in job.result_schema]
        df = pd.DataFrame(data, columns=columns)
    return df
```

To export tables from Python to TD:

```python
client = pytd.Client(apikey=td_api_key, endpoint=td_api_server, database=database)
client.load_table_from_dataframe(df, 'td table name', writer='bulk_import', if_exists='append')
```

It is best practice to always include `session_id` as a column in every table to track multiple model runs.
