select a.id as action_id,
       a.created_at as action_date,
       a.user_id as mvr_id,
       u.last_name as mvr_last_name,
       u.first_name as mvr_first_name,
       u.email as mvr_email,
       a.registration_id as registration_record_id,
       r.status as registration_status,
       r.last_name as resident_last_name,
       r.first_name as resident_first_name,
       r.created_at as registration_created_on,
       r.county, r.office, a.action_details,
       a.office as action_office
from {{ ref('v_transform_action')}} a
         left outer join {{ ref('v_transform_user')}} u on a.user_id=u.id
         left outer join {{ ref('v_transform_registration')}} r on r.id=a.registration_id
where a.registration_id is not null