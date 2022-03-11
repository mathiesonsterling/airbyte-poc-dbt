SELECT
    p.transaction_id,
    b.ip_address,
    {{ r_transform_ipv6to4('b.ip_address')}} as lowest_version_ip_address,
    b.created_at,
    b.event_type,
    CONCAT(p.first_name, ' ', p.last_name) AS full_name,
    p.phone,
    p.email,
    CONCAT(p.address, ', ', p.city, ', ', p.state, ' ', p.zip) AS address,
    p.county
FROM
{{ ref('v_transform_permit_test_block_event_log')}} b JOIN {{ ref('v_transform_permit')}} p
    ON b.permit_id = p.id