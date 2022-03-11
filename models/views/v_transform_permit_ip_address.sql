WITH permit_ip_count AS (
     SELECT
         id,
         count(distinct ip_address) as ip_count
     FROM
         {{ ref('v_permit_ip_address_start') }}
     GROUP BY
         id
)

SELECT
    permit_ip_address.*,
    pic.ip_count
FROM
    {{ ref('v_permit_ip_address_start') }} permit_ip_address
        JOIN
    permit_ip_count pic ON permit_ip_address.id = pic.id
ORDER BY
    permit_ip_address.id