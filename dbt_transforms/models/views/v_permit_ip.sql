SELECT
    distinct l.permit_id,
             l.ip_address,
             any_value(l.event_type = 'REGISTER_TEST') as is_register_test_ip, -- this is the IP address the blocking code uses
             min(l.occurred_at) as ip_first_seen,
             max(l.occurred_at) as ip_last_seen
FROM
    {{ ref('v_transform_permit_event_log') }} l
WHERE
    l.event_type not in ('PASS_TEST', 'FAIL_TEST') -- omit server IP addresses
GROUP BY
    l.permit_id, l.ip_address