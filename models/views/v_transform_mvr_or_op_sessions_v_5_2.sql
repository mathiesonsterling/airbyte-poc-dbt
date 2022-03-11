#standardSQL
WITH
  TRANSACTION_ACTIONS AS (
  SELECT
    user_id,
    registration_id AS transaction_id,
    action_details,
    created_at,
    CASE
      WHEN action_details LIKE '%requested%' THEN 'INFO_REQUESTED'
      WHEN action_details LIKE '%invoice%' THEN 'INVOICED'
      WHEN action_details LIKE '%issued plates%' THEN 'FULFILLED'
      WHEN action_details LIKE '%rejected%' THEN 'REJECTED'
      WHEN action_details LIKE '%assign%' THEN 'ASSIGN_UNASSIGN'
      WHEN action_details LIKE '%set a status of%' THEN 'DOCUMENT_REVIEW'
    ELSE
    'N/A'
  END
    AS status,
    'ORIGINAL_REGISTRATION' AS transaction_type
  FROM
    {{ ref('v_transform_action') }}
  WHERE
    registration_id IS NOT NULL
  UNION ALL
  SELECT
    user_id,
    permit_id AS transaction_id,
    action_details,
    created_at,
    CASE
      WHEN action_details LIKE '%requested%' THEN 'INFO_REQUESTED'
      WHEN action_details LIKE '%approved%' THEN 'APPROVED'
      WHEN action_details LIKE '%assign%' THEN 'ASSIGN_UNASSIGN'
      WHEN action_details LIKE '%set a status of%' THEN 'DOCUMENT_REVIEW'
    ELSE
    'N/A'
  END
    AS status,
    'ORIGINAL_PERMIT' AS transaction_type
  FROM
    {{ ref('v_transform_action') }}
  WHERE
    permit_id IS NOT NULL )
SELECT
    user_id,
    first_name,
    last_name,
    u.office as user_office,
    (SELECT CAST(FLOOR(SUM (
            CASE WHEN difference <= 30 * 60000 THEN difference/60000
                 WHEN difference IS NULL THEN 0
                 ELSE 1
                END)) AS INT64) as worked_mins
     FROM ( SELECT
                TIMESTAMP_DIFF(
                        created_at,
                        LAG(created_at) OVER(order by created_at),
                        MILLISECOND) AS difference
            FROM (SELECT ta.created_at AS created_at FROM TRANSACTION_ACTIONS ta ORDER BY created_at)
          )
    )
        as worked_mins,
    MIN(DATETIME(ta.created_at,
                 'America/New_York')) AS session_start,
    MAX(DATETIME(ta.created_at,
                 'America/New_York')) AS session_end,
    DATE_DIFF(MAX(DATETIME(ta.created_at,
                           'America/New_York')), MIN(DATETIME(ta.created_at,
                                                              'America/New_York')), MINUTE) AS total_session_in_minutes,
    DATE(ta.created_at, 'America/New_York') AS day,
    COUNT(*) AS total_per_day,
    COUNTIF(transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_count,
    COUNTIF(ta.status = 'INFO_REQUESTED'
    AND transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_info_requested,
    COUNTIF(ta.status = 'INVOICED'
    AND transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_invoiced,
    COUNTIF(ta.status = 'FULFILLED'
    AND transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_fulfilled,
    COUNTIF(ta.status = 'REJECTED'
    AND transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_rejected,
    COUNTIF(ta.status = 'ASSIGN_UNASSIGN'
    AND transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_assign_unassign,
    COUNTIF(ta.status = 'DOCUMENT_REVIEW'
    AND transaction_type = 'ORIGINAL_REGISTRATION') AS original_registration_document_review,
    COUNTIF(transaction_type = 'ORIGINAL_PERMIT') AS original_permit_count,
    COUNTIF(ta.status = 'INFO_REQUESTED'
    AND transaction_type = 'ORIGINAL_PERMIT') AS original_permit_info_requested,
    COUNTIF(ta.status = 'APPROVED'
    AND transaction_type = 'ORIGINAL_PERMIT') AS original_permit_approved,
    COUNTIF(ta.status = 'ASSIGN_UNASSIGN'
    AND transaction_type = 'ORIGINAL_PERMIT') AS original_permit_assign_unassign,
    COUNTIF(ta.status = 'DOCUMENT_REVIEW'
    AND transaction_type = 'ORIGINAL_PERMIT') AS original_permit_document_review,
FROM
    TRANSACTION_ACTIONS ta
    INNER JOIN
    {{ ref('v_transform_user') }} u
ON
    u.id = ta.user_id
    AND ta.status != 'N/A'
GROUP BY
    user_id,
    day,
    first_name,
    last_name,
    u.office
