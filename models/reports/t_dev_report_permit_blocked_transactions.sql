SELECT
    ip.ip_address,
    {{ r_transform_ipv6to4('ip.ip_address')}} as lowest_version_ip_address,
    ipc.num_permits,
    p.*
FROM
        {{ ref('v_ip_block_permit') }} ip
    JOIN
        {{ref('v_ip_block_permit_count')}} ipc
    ON
        ip.ip_address = ipc.ip_address
    JOIN
        {{ ref('v_transform_permit') }} p
    ON
        ip.permit_id = p.id