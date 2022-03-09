SELECT
    i.id AS invoice_id,
    i.user_id AS invoice_user_id,
    i.sales_tax_id,
    invoice_number,
    i.status AS invoice_status,
    amount,
    i.initials AS invoice_initials,
    i.office AS invoice_office,
    i.created_at AS invoice_created_at,
    i.updated_at AS invoice_updated_at,
    client_id,
    r.user_id AS registration_user_id,
    transaction_id,
    r.status AS registration_status,
    r.first_name,
    r.last_name,
    rejection_reason,
    r.unread_psb_messages AS unread_mvr_messages,
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
    a.created_at AS mvr_fulfilled_at,
    a.action_details
FROM {{ ref('v_transform_invoice')}} i
         JOIN {{ ref('v_transform_sales_tax')}} r
              ON CAST(i.sales_tax_id AS INT64) = r.id
         LEFT JOIN {{ ref('v_transform_action')}} a
                   ON r.id = a.sales_tax_id
         LEFT JOIN {{ ref('v_transform_user')}} u
                   ON i.user_id = u.id
WHERE
        a.action_details LIKE "%fulfilled this sales tax.%"