-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH internal_note AS (
    SELECT * EXCEPT (ingested_at, rank) FROM (
SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_internal_note')}}) WHERE rank=1
    )

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT DISTINCT * FROM internal_note t1 WHERE created_at = (SELECT MAX(created_at) FROM internal_note t2 WHERE t1.id =t2.id) ORDER BY created_at DESC
