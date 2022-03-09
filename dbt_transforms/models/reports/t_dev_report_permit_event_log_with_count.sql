SELECT
    l.id,
    p.transaction_id,
    l.correlation_id,
    l.event_type,
    l.uri,
    l.x_forwarded_for,
    l.ip_address,
    l.user_agent,
    l.created_at,
    l.occurred_at,
    -- Convert ip_address to binary, then to hex string, then check the left 12 bytes (2 chars/byte) for "IPv4 as IPv6" value
    CASE SUBSTR(CAST(NET.IP_FROM_STRING(l.ip_address) AS STRING FORMAT 'HEX'), 0, 24)
        WHEN '00000000000000000000ffff' THEN
            -- Convert the last 4 bytes back to an ip_address, getting us the IPv4 format
            NET.IP_TO_STRING(CAST(SUBSTR(CAST(NET.IP_FROM_STRING(l.ip_address) AS STRING FORMAT 'HEX'), 25, 8) AS BYTES FORMAT 'HEX'))
        ELSE
            -- else we have an actual IPv6 value and cannot convert to IPv4
            l.ip_address
        END
                                           lowest_version_ip_address,
    coalesce(pt.num_ip_pass_test_events,
             0) num_ip_pass_test_events,
    COUNTIF(l.event_type LIKE 'PASS_TEST') OVER(PARTITION BY l.ip_address ORDER BY l.occurred_at ROWS BETWEEN UNBOUNDED PRECEDING AND 0 PRECEDING) num_test_pass_events_for_ip_address
FROM
    {{ ref('v_transform_permit_event_log')}} l
        JOIN
    {{ ref('v_transform_permit')}} p
    ON
            p.id = l.permit_id
        LEFT JOIN (
        SELECT
            l2.ip_address,
            COUNT(DISTINCT l2.permit_id) AS num_ip_pass_test_events
        FROM
            {{ ref('v_transform_permit_event_log')}} l2
        WHERE
                event_type = 'PASS_TEST'
        GROUP BY
            l2.ip_address ) AS pt
                  ON
                          l.ip_address = pt.ip_address