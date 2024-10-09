select
date_format(now(), '%Y-%m-%d %H:%i') as runtime,
${session_id} as session_id,
feature,
weight
FROM cltv_regressor