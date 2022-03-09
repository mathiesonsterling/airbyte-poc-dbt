SELECT *,
       CONCAT(first_name, ' ', last_name) AS full_name
FROM {{ ref('v_transform_mvr_or_op_sessions_v_5_2') }}