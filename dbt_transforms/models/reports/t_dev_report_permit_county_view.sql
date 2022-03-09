SELECT  distinct

    p.transaction_id,
    p.first_name,
    p.last_name,
    CONCAT(p.first_name, " ", p.last_name) AS full_name,
    p.phone,
    CONCAT(SUBSTR(p.phone, 0, 3),"-",SUBSTR(p.phone,3,3),"-",SUBSTR(p.phone, 7,4)) AS phone_number,
    p.submitted_on,
    p.status,
    p.appointment_time,
    nt.note_type,
    n.note_content,
    c.name as county,
    c.state_county,
    n.created_at as note_added,
    DATETIME(n.created_at, "America/New_York") AS date_added,
    p.appointment_county

FROM {{ ref('v_transform_permit')}} p

         join  {{ ref('v_transform_internal_note')}} n on n.permit_id = p.id

         join {{ ref('v_transform_internal_note_type')}} nt on n.note_type_id = nt.id

         join {{ ref('v_transform_county')}} c on c.id = p.county_id