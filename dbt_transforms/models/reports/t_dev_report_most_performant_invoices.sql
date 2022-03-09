select
    i.id as invoice_id,
    i.user_id as invoice_user_id,
    i.registration_id,
    invoice_number,
    i.status as invoice_status,
    amount,
    i.initials as invoice_initials,
    i.office as invoice_office,
    i.created_at as invoice_created_at,
    i.updated_at as invoice_updated_at,
    i.payment_type as invoice_payment_type,

    resident_id,
    r.user_id as registration_user_id,
    transaction_id,
    r.status as registration_status,
    r.first_name,
    r.last_name,
    r.email,
    phone,
    county,
    r.identification_number,
    plates_needed,
    plates_delivery,
    plates,
    notes,
    rejection_reason,
    archived,
    r.initials as registration_initials,
    UPPER(r.initials) AS mvr_initials,
    r.office as registration_office,
    r.created_at as registration_created_at,
    r.updated_at as registration_updated_at,

    u.id as mvr_id,
    u.first_name as mvr_first_name,
    u.last_name as mvr_last_name,
    u.email as mvr_email,
    u.status as mvr_status,
    u.created_at as mvr_created_at,
    u.updated_at as mvr_updated_at,
    r.fulfilled_on as mvr_fulfilled_at,

    b.identification_number as boat_identification_number

from {{ ref('v_transform_invoice')}} i
         join {{ ref('v_transform_registration')}} r on CAST(i.registration_id AS INT64) = r.id
         left join {{ ref('v_transform_user')}} u on i.user_id = u.id
         left join {{ ref('v_transform_boat_information')}} b on r.id = b.registration_id
where (fulfilled_on is not null)
  and (r.status like "%Fulfilled%" or r.status like "%TitleReceived%")