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
    {{ ref('v_transform_action')}}
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
    {{ ref('v_transform_action')}}
WHERE
    permit_id IS NOT NULL