SELECT
    a.id AS action_id,
    a.created_at AS action_date,
    DATETIME(a.created_at, "America/New_York") AS action_date_est,
    a.user_id AS mvr_id,
    u.last_name AS mvr_last_name,
    u.first_name AS mvr_first_name,
    CONCAT(u.last_name,", ", u.first_name) AS mvr_fullname,
    u.email AS mvr_email,
    a.permit_id AS permit_record_id,
    p.status AS permit_status,
    p.last_name AS resident_last_name,
    p.first_name AS resident_first_name,
    p.gender AS resident_gender,
    p.created_at AS permit_created_on,
    DATETIME(p.created_at, "America/New_York") AS permit_created_on_est,
    p.county,
    p.county_id,
    p.zip,
    p.office,
    a.action_details
FROM
    {{ ref('v_transform_action')}} a
        LEFT OUTER JOIN
    {{ ref('v_transform_user')}} u
    ON
            a.user_id=u.id
        LEFT OUTER JOIN
    {{ ref('v_transform_permit')}} p
    ON
            p.id=a.permit_id
WHERE
    a.permit_id IS NOT NULL