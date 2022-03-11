SELECT
    p.transaction_id,
    p.id,
    p.status,
    p.test_status,
    p.test_link_date,
    p.test_date,
    p.submitted_on,
    p.archived,
    --  p.archived_at, -- Not in the BQ table, may need to be refreshed.
    p.on_hold,
    p.on_hold_reason,
    p.rejection_reason,
    p.created_at,
    p.updated_at,

    pip.ip_address,
    CASE SUBSTR(CAST(NET.IP_FROM_STRING(pip.ip_address) AS STRING FORMAT 'HEX'), 0, 24)
        WHEN '00000000000000000000ffff' THEN
            -- Convert the last 4 bytes back to an ip_address, getting us the IPv4 format
            NET.IP_TO_STRING(CAST(SUBSTR(CAST(NET.IP_FROM_STRING(pip.ip_address) AS STRING FORMAT 'HEX'), 25, 8) AS BYTES FORMAT 'HEX'))
        ELSE
            -- else we have an actual IPv6 value and cannot convert to IPv4, or it was already IPv4
            pip.ip_address
        END AS lowest_version_ip_address,
    pip.is_register_test_ip,
    pip.ip_first_seen,
    pip.ip_last_seen,

    p.appointment_county,
    p.appointment_location_id,
    p.appointment_time,
    p.appointment_reference_number,

    p.first_name,
    p.last_name,
    p.email,
    p.phone,
    p.address,
    p.city,
    p.county
FROM
    {{ ref('v_transform_permit') }} p
    JOIN
    {{ ref('v_permit_ip') }} pip ON p.id = pip.permit_id