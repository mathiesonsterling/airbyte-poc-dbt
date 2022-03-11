select distinct
    i.id as invoice_id,
    i.user_id as invoice_user_id,
    i.registration_id,
    invoice_number,
    i.status as invoice_status,
    amount,
    payment_type,

    i.initials as invoice_initials,
    i.office as invoice_office,
    i.created_at as invoice_created_at,
    DATETIME(i.created_at, "America/New_York") as invoice_created_at_est,
    i.updated_at as invoice_updated_at,
    DATETIME(i.updated_at, "America/New_York") as invoice_updated_at_est,

    resident_id,
    r.user_id as registration_user_id,
    transaction_id,
    r.status as registration_status,
    r.first_name,
    r.last_name,
    r.email,
    phone,
    r.county,
    r.identification_number,
    plates_needed,
    plates_delivery,
    plates,
    notes,
    rejection_reason,
    archived,
    r.initials as registration_initials,
    r.office as registration_office,
    r.created_at as registration_created_at,
    DATETIME(r.created_at, "America/New_York") as registration_created_at_est,
    r.updated_at as registration_updated_at,
    DATETIME(r.updated_at, "America/New_York") as registration_updated_at_est,

    u.id as mvr_id,
    u.first_name as mvr_first_name,
    u.last_name as mvr_last_name,
    u.email as mvr_email,
    u.status as mvr_status,
    u.created_at as mvr_created_at,
    u.updated_at as mvr_updated_at,

    b.identification_number as boat_identification_number,
    ra.line_one,
    ra.apartment_number,
    ra.city,
    ra.zip,
    ra.state


from {{ ref('v_transform_invoice')}} i
         left join {{ ref('v_transform_registration')}} r on  r.id = CAST(i.registration_id AS INT64)
         left join {{ ref('v_transform_user')}} u on u.id = i.user_id
         left join {{ ref('v_transform_boat_information')}} b on r.id = b.registration_id
         left outer join {{ ref('v_transform_registration_address')}} ra on ra.registration_id = r.id
    and ra.address_type = "MAILING"