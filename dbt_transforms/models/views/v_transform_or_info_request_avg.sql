SELECT
    registration_approved_info_requested.approved_date,
    COUNT(registration_approved_info_requested) AS approved_transactions,
    SUM(registration_approved_info_requested.missing_docs_count ) AS info_req_count,
    ROUND(AVG(registration_approved_info_requested.missing_docs_count), 1) AS avg_info_req,
    MAX(registration_approved_info_requested.missing_docs_count ) AS max_info_req
FROM (
         SELECT
             r.transaction_id,
             DATE(approved.created_at) AS approved_date,
     COUNT(missing.registration_id) missing_docs_count
    FROM
    {{ ref('v_transform_registration')}} r
  JOIN (
    SELECT
      registration_id,
      MAX(created_at) created_at
    FROM
      {{ ref('v_transform_action')}}
    WHERE
      action_details LIKE "%issued plates%"
    GROUP BY
      registration_id ) AS approved
ON
    r.id = approved.registration_id
    LEFT JOIN (
    SELECT
    registration_id,
    created_at
    FROM
    {{ ref('v_transform_action')}}
    WHERE
    action_details LIKE "%missing documents%" ) AS missing
    ON
    r.id = missing.registration_id
WHERE
    r.status IN ("Fulfilled",
    "TitleReceived")
GROUP BY
    r.transaction_id,
    r.created_at,
    DATE(approved.created_at) ) AS registration_approved_info_requested
GROUP BY
    registration_approved_info_requested.approved_date
ORDER BY
    registration_approved_info_requested.approved_date DESC