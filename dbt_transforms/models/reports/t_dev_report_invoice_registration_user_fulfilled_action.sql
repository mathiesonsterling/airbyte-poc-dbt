SELECT
    i.id AS invoice_id,
    i.user_id AS invoice_user_id,
    a.office AS action_office,
    i.registration_id,
    invoice_number,
    i.status AS invoice_status,
    amount,
    i.initials AS invoice_initials,
    i.office AS invoice_office,
    i.created_at AS invoice_created_at,
    i.updated_at AS invoice_updated_at,
    resident_id,
    r.user_id AS registration_user_id,
    transaction_id,
    r.status AS registration_status,
    r.first_name,
    r.last_name,
    r.email,
    phone,
    county,
    identification_number,
    plates_needed,
    plates_delivery,
    plates,
    notes,
    rejection_reason,
    archived,
    r.initials AS registration_initials,
    r.office AS registration_office,
    r.created_at AS registration_created_at,
    r.updated_at AS registration_updated_at,
    u.id AS mvr_id,
    u.first_name AS mvr_first_name,
    u.last_name AS mvr_last_name,
    u.email AS mvr_email,
    u.status AS mvr_status,
    u.created_at AS mvr_created_at,
    u.updated_at AS mvr_updated_at,
    a.created_at AS mvr_fulfilled_at
FROM
    {{ ref('v_transform_invoice')}} i
        JOIN
    {{ ref('v_transform_registration') }} r
    ON
            CAST(i.registration_id AS INT64) = r.id
        LEFT JOIN
    {{ ref('v_transform_action')}} a
    ON
            r.id = a.registration_id
        LEFT JOIN
    {{ ref('v_transform_user')}} u
    ON
            i.user_id = u.id
WHERE
    (a.action_details LIKE "%issued plates % for this registration.%")
  AND (r.status LIKE "Fulfilled"
    OR r.status LIKE "TitleReceived")