WITH events AS (
    SELECT
        permit_id,
        ip_address,
        'permit-event' AS log_type,
        event_type,
        {{ r_transform_ipv6to4('ip_address') }} AS lowest_version_ip_address,
        occurred_at
    FROM
        {{ ref('v_transform_permit_event_log')}} l
    UNION ALL
    SELECT
        permit_id,
        ip_address,
        'permit-test-block-event' AS log_type,
        event_type,
        {{ r_transform_ipv6to4('ip_address') }} AS lowest_version_ip_address,
        created_at AS occurred_at
    FROM
        {{ ref('v_transform_permit_test_block_event_log')}} bl
    ORDER BY
        occurred_at
), timeline_events AS (
    SELECT
        p.transaction_id,
        e.*,
        DATE_DIFF(occurred_at, LAG(occurred_at) OVER (PARTITION BY permit_id ORDER BY occurred_at), hour) AS diff_hours,
        DATE_DIFF(occurred_at, LAG(occurred_at) OVER (PARTITION BY permit_id ORDER BY occurred_at), minute) AS diff_minutes,
        DATE_DIFF(occurred_at, LAG(occurred_at) OVER (PARTITION BY permit_id ORDER BY occurred_at), second) AS diff_seconds
    FROM
        events e
    JOIN {{ ref('v_transform_permit')}} p ON e.permit_id = p.id
), W AS (
SELECT
    t.transaction_id,
    t.ip_address,
    t.log_type,
    t.event_type,
    t.lowest_version_ip_address,
    t.occurred_at,
    t.diff_hours AS hours,
    t.diff_minutes - (t.diff_hours * 60) AS minutes,
    t.diff_seconds - (t.diff_minutes * 60 ) - (t.diff_hours * 3600) AS seconds
FROM
    timeline_events t )

SELECT
    w.transaction_id,
    w.ip_address,
    w.log_type,
    w.event_type,
    w.lowest_version_ip_address,
    w.occurred_at,
    TRIM( CONCAT(
            IF
                (w.hours > 0,
                 CONCAT(w.hours, ' hr '),
                 ''),
            IF
                (w.minutes > 0,
                 CONCAT(w.minutes, ' min '),
                 ''),
            IF
                (w.seconds > 0
                     OR (w.hours = 0
                    AND w.minutes = 0),
                 CONCAT(w.seconds, ' sec'),
                 '') ) ) AS human_time_since_last_event
FROM w