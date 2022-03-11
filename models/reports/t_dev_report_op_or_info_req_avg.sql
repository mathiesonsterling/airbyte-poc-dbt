SELECT
    p.approved_date AS permit_approved_date,
    p.approved_transactions AS permit_approved_transactions,
    p.info_req_count AS permit_info_req_count,
    p.avg_info_req AS permit_avg_info_req,
    p.max_info_req AS permit_max_info_req,
    r.approved_date AS registration_approved_date,
    r.approved_transactions AS registration_approved_transactions,
    r.info_req_count AS registration_info_req_count,
    r.avg_info_req AS registration_avg_info_req,
    r.max_info_req AS registration_max_info_req,
FROM
    {{ ref('v_transform_op_info_request_avg')}} p
        FULL JOIN
    {{ ref('v_transform_or_info_request_avg')}} r
    ON
            p.approved_date = r.approved_date
ORDER BY
    permit_approved_date DESC,
    registration_approved_date DESC
