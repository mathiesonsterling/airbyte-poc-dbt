SELECT
    p.transaction_id,
    ip.ip_address,
    {{ r_transform_ipv6to4('ip.ip_address')}} as lowest_version_ip_address,
    p.status,
    p.test_status,
    p.submitted_on,
    p.archived,
--  p.archived_at, -- Not in the BQ table, may need to be refreshed.
    p.on_hold,
    p.on_hold_reason,
    p.rejection_reason,
    p.appointment_county,
    p.appointment_location_id,
    p.appointment_time,
    p.appointment_reference_number,
    p.created_at,
    p.updated_at,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, " ", p.last_name) AS full_name,
    p.email,
    p.phone,
    p.test_date,
    p.address,
    p.city,
    p.county
FROM
    {{ ref('v_transform_permit')}} p
JOIN {{ ref('v_permit_ip_address') }} ip ON p.id = ip.permit_id
ORDER BY
    ip.ip_address, p.created_at