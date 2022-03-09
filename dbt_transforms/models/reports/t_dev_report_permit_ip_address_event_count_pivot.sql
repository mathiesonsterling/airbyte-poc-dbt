SELECT
    permit_ip.ip_address,
    {{ r_transform_ipv6to4('permit_ip.ip_address') }} as lowest_version_ip_address_macro,
    SUM(PASS_TEST) AS PASS_TEST,
    SUM(FAIL_TEST) AS FAIL_TEST,
    SUM(REGISTER_TEST) AS REGISTER_TEST,
    SUM(SUBMIT) AS SUBMIT,
    SUM(UPDATE) AS UPDATE,
    SUM(READ) AS READ
FROM ( SELECT * FROM {{ ref('v_transform_permit_event_log')}} )
    PIVOT(COUNT(*) FOR event_type IN ('PASS_TEST', 'FAIL_TEST', 'REGISTER_TEST', 'SUBMIT', 'UPDATE', 'READ')) piv
    LEFT JOIN {{ ref('v_register_test_permit_ip') }} permit_ip
on piv.permit_id = permit_ip.permit_id
GROUP BY
    ip_address
ORDER BY
    PASS_TEST desc