WITH ip_test_status_summary as (
    SELECT
        ip_address,
        sum(all_ip_test_not_started) as all_ip_test_not_started,
        sum(all_ip_test_started) as all_ip_test_started,
        sum(all_ip_test_failed) as all_ip_test_failed,
        sum(all_ip_test_passed) as all_ip_test_passed,
        sum(register_test_ip_test_not_started) as register_test_ip_test_not_started,
        sum(register_test_ip_test_started) as register_test_ip_test_started,
        sum(register_test_ip_test_failed) as register_test_ip_test_failed,
        sum(register_test_ip_test_passed) as register_test_ip_test_passed,
    FROM {{ ref('v_transform_permit_ip_address')}}
             PIVOT (count(distinct id) as all_ip, countif(is_register_test_ip) as register_test_ip FOR test_status in (
            'N/A' as test_not_started,
            'Test Started' as test_started,
            'Failed' as test_failed,
            'Passed' as test_passed)) as piv
GROUP BY piv.ip_address
    )

SELECT DISTINCT
    pia.ip_address,
    CASE SUBSTR(CAST(NET.IP_FROM_STRING(pia.ip_address) AS STRING FORMAT 'HEX'), 0, 24)
        WHEN '00000000000000000000ffff' THEN
            -- Convert the last 4 bytes back to an ip_address, getting us the IPv4 format
            NET.IP_TO_STRING(CAST(SUBSTR(CAST(NET.IP_FROM_STRING(pia.ip_address) AS STRING FORMAT 'HEX'), 25, 8) AS BYTES FORMAT 'HEX'))
        ELSE
            -- else we have an actual IPv6 value and cannot convert to IPv4, or it was already IPv4
            pia.ip_address
        END AS lowest_version_ip_address,
    s.all_ip_test_not_started + s.all_ip_test_started + s.all_ip_test_failed + s.all_ip_test_passed as all_ip_permit_count,
    s.all_ip_test_not_started,
    s.all_ip_test_started,
    s.all_ip_test_failed,
    s.all_ip_test_passed,

    s.register_test_ip_test_not_started + s.register_test_ip_test_started + s.register_test_ip_test_failed + s.register_test_ip_test_passed as register_test_ip_permit_count,
    s.register_test_ip_test_not_started,
    s.register_test_ip_test_started,
    s.register_test_ip_test_failed,
    s.register_test_ip_test_passed
FROM
    {{ ref('v_transform_permit_ip_address')}} pia
        LEFT JOIN ip_test_status_summary s
                  ON pia.ip_address = s.ip_address
ORDER BY
    s.register_test_ip_test_passed desc, s.all_ip_test_passed desc