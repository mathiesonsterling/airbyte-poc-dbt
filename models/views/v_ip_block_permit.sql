SELECT
    l.ip_address,
    l.permit_id
FROM
    {{ ref('v_transform_permit_test_block_event_log') }} l
WHERE
    l.event_type = 'BLOCK'
GROUP BY
    l.ip_address,
    l.permit_id