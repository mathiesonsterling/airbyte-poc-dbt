WITH purge_report_stats AS (
    SELECT
        transaction_type,
    Date(created_at) as day,
    sum(distinct(num_records)) as total
FROM {{ ref('v_transform_purge_report')}}
group by
    day,
    transaction_type
    )
SELECT *,
       IF(transaction_type ="Permit", day, NULL) AS permit_days,
       IF(transaction_type ="Registration", day, NULL) AS registration_days
FROM purge_report_stats