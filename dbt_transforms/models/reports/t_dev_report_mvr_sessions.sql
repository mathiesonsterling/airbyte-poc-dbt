WITH mvr_sessions AS (
    SELECT
        first_name,
        last_name,
    DATE(a.created_at, 'America/New_York') AS Day,
    MIN(DATETIME(a.created_at, 'America/New_York')) AS session_start,
    MAX(DATETIME(a.created_at, 'America/New_York')) AS session_end,
    DATE_DIFF(MAX(DATETIME(a.created_at, 'America/New_York')),
    MIN(DATETIME(a.created_at, 'America/New_York')), MINUTE) AS total_session_in_minutes,
    COUNT(a.id) AS activity_total_count
FROM
    {{ ref('v_transform_action')}} a
    LEFT JOIN
    {{ ref('v_transform_user')}} u
ON
    a.user_id = u.id
WHERE
    DATE(a.created_at, 'America/New_York') > CURRENT_DATE() - 30
  and action_details not like '%assigned%'
GROUP BY
    first_name,
    last_name,
    Day
    )

SELECT
    *,
    CONCAT(last_name, ", ", first_name) AS mvr_fullname,
    CONCAT(FLOOR(total_session_in_minutes/60),"h"," ",MOD(total_session_in_minutes,60), "m") as total_session_time,
FROM mvr_sessions