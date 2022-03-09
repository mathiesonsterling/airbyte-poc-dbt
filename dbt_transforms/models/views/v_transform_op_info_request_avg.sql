SELECT
    permit_approved_info_requested.approved_date,
    COUNT(permit_approved_info_requested) AS approved_transactions,
    SUM(permit_approved_info_requested.missing_docs_count ) AS info_req_count,
    ROUND(AVG(permit_approved_info_requested.missing_docs_count), 1) AS avg_info_req,
    MAX(permit_approved_info_requested.missing_docs_count ) AS max_info_req
FROM (
         SELECT
             p.transaction_id,
             DATE(approved.created_at) AS approved_date,
     COUNT(missing.permit_id) missing_docs_count
    FROM
    {{ ref('v_transform_permit') }} p
  JOIN (
    SELECT
      permit_id,
      created_at
    FROM
      {{ ref('v_transform_action') }}
    WHERE
      action_details LIKE "%approved this permit%" ) AS approved
ON
    p.id = approved.permit_id
    LEFT JOIN (
    SELECT
    permit_id,
    created_at
    FROM
    {{ ref('v_transform_action') }}
    WHERE
    action_details LIKE "%missing documents%" ) AS missing
    ON
    p.id = missing.permit_id
WHERE
    p.status="Fulfilled"
GROUP BY
    p.transaction_id,
    p.created_at,
    DATE(approved.created_at) ) AS permit_approved_info_requested
GROUP BY
    permit_approved_info_requested.approved_date
ORDER BY
    permit_approved_info_requested.approved_date DESC
