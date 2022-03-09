select date(op_status_avg_time.approved_date) as approved_date,
    round(avg(op_status_avg_time.created_to_submitted ),1) as created_to_submitted_avg_mins,
    round(avg(op_status_avg_time.submitted_to_test_passed ),1) as submitted_to_test_passed_avg_hours,
    round(avg(op_status_avg_time.test_passed_to_approved ),1) as test_passed_to_approved_avg_days,
    round(avg(op_status_avg_time.created_to_approved ),1) as created_to_approved_avg_days,
from (
    select  p.transaction_id,
    approved.created_at approved_date,
    ROUND(date_diff(p.submitted_on, p.created_at, SECOND)/60,1) as created_to_submitted,
    ROUND(date_diff(p.test_date, p.submitted_on, MINUTE)/60,1) as submitted_to_test_passed,
    ROUND(date_diff(approved.created_at, p.test_date, HOUR)/24,1) as test_passed_to_approved,
    ROUND(date_diff(approved.created_at, p.created_at, HOUR)/24,1) as created_to_approved,
    from {{ ref('v_transform_permit')}} p
    join (
    select permit_id, created_at  from {{ ref('v_transform_action')}} where action_details like "%approved this permit%"
    ) as approved on p.id = approved.permit_id
    left join (
    select permit_id, min(created_at)  from {{ ref('v_transform_action')}} where action_details like "%missing documents%" group by permit_id
    ) as first_info_req on p.id = first_info_req.permit_id
    left join (
    select permit_id, max(created_at)  from {{ ref('v_transform_message')}} where content like "%Client uploaded%" group by permit_id
    ) as last_info_rec on p.id = last_info_rec.permit_id
    where p.status="Fulfilled"
    order by approved.created_at desc) as op_status_avg_time
group by approved_date
order by approved_date desc