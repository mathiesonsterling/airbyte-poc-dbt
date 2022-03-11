SELECT
    i.amount,
    i.status AS invoice_status,
    i.created_at,
    i.updated_at,
    i.invoice_number,
    r.transaction_id,
    r.status AS registration_status,
    r.first_name,
    r.last_name
FROM {{ ref('v_transform_invoice') }} i
        JOIN
    {{ ref('v_transform_registration') }} r
    ON
            r.id = i.registration_id
WHERE
        i.payment_type LIKE "%Cash%"
