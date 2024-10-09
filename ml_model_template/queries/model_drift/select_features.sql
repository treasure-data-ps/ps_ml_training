select feature as column_name
FROM ${feature_importances_table}
WHERE session_id=(SELECT max(session_id) FROM ${feature_importances_table})
ORDER by time desc, abs(weight) desc
limit ${top_n_features}
