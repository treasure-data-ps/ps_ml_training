drop table if EXISTS${globals.sink_database}.pred_and_actual_values;
create table ${globals.sink_database}.pred_and_actual_values as(
with prediction as(
SELECT customerid, predicted_target from ${globals.sink_database}.predictions
)
SELECT ct.customerid , ct.cltv as actual_target, p.predicted_target from ${globals.sink_database}.cltv_test ct 
INNER JOIN prediction p on p.customerid=ct.customerid
)