SELECT status, count(*) AS total, date(current_date()) AS date, "Registration" AS transaction_type
FROM {{ ref('v_transform_registration') }}
GROUP BY status

UNION ALL
SELECT status, count(*) AS total, date(current_date()) AS date, "Permit" AS transaction_type
FROM {{ ref('v_transform_permit') }}
GROUP BY status