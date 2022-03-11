SELECT
    DISTINCT permit_id, ip_address
FROM {{ ref('v_transform_permit_event_log')}}
WHERE
-- Don't include 3rd party server IP addresses
event_type not in ('PASS_TEST', 'FAIL_TEST')