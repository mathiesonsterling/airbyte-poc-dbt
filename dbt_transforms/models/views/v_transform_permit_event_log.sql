-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH permit_event_log AS (
    SELECT * EXCEPT (ingested_at, rank) FROM (
SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_permit_event_log')}}) WHERE rank=1
    )

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT DISTINCT * FROM permit_event_log t1 WHERE created_at = (SELECT MAX(created_at) FROM permit_event_log t2 WHERE t1.id =t2.id) ORDER BY created_at DESC
