SELECT array_join(array_distinct(array_agg(transf_code)), CONCAT(', ',chr(10))) AS transf_code,
       array_join(array_distinct(array_agg(regular_code)), CONCAT(', ',chr(10))) AS regular_code
FROM schema