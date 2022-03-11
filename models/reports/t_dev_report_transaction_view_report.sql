SELECT
    case
        when a.registration_id is not null then r.transaction_id
        when a.permit_id is not null then p.transaction_id
        when a.non_driver_id_id is not null then n.transaction_id
        when a.sales_tax_id is not null then s.transaction_id
        when a.license_amendment_id is not null then la.transaction_id
        when a.license_reciprocity_id is not null then lr.transaction_id
        end as transaction_id,
    a.id,
    a.created_at,
    a.action_details,
    u.first_name,
    u.last_name,
    CONCAT(u.first_name, " ", u.last_name) AS name

FROM {{ ref('v_transform_action')}} a
         full join {{ ref('v_transform_user')}} u on a.user_id = u.id
         full join {{ ref('v_transform_registration')}} r on a.registration_id = r.id
         full join {{ ref('v_transform_permit')}} p on permit_id = p.id
         full join {{ ref('v_transform_non_driver_id')}} n on non_driver_id_id = n.id
         full join {{ ref('v_transform_sales_tax')}} s on s.id = a.sales_tax_id
         full join {{ ref('v_transform_license_amendment')}} la on la.id = a.license_amendment_id
         full join {{ ref('v_transform_license_reciprocity')}} lr on lr.id = a.license_reciprocity_id
where a.action_details like "%viewed%"