SELECT ${unique_user_id},
 customer_tier,
 gender,
 age
FROM ${table.src_db}.${table.src_table}
${table.custom_filter}