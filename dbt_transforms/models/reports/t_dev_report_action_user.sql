select
    a.id as action_id,
    u.id as user_id,
    a.registration_id,
    a.permit_id,
    a.sales_tax_id,
    a.non_driver_id_id,
    a.license_amendment_id,
    a.license_reciprocity_id,
    a.action_details,
    a.created_at as action_created_at,
    u.first_name,
    u.last_name,
    u.email,
    u.status as user_status,
    u.created_at as user_created,
    u.updated_at as user_updated
from {{ ref('v_transform_action') }} a
         join {{ ref('v_transform_user') }} u on a.user_id = u.id