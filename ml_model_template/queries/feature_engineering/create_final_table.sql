WITH T1 as(
SELECT *
FROM ${tbl}
)
SELECT
${input_data.canonical_id},
${target_name},
${td.last_results.regular_code}
FROM T1