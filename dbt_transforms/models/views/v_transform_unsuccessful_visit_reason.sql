SELECT * EXCEPT (ingested_at, rank) FROM (
        SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_unsuccessful_visit_reason')}}) WHERE rank=1
