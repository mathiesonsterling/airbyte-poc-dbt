SELECT DISTINCT permit_id, ip_address
FROM {{ ref('v_transform_permit_event_log')}}
WHERE event_type = 'REGISTER_TEST'