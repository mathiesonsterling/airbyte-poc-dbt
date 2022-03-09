-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH permit AS (
    SELECT * EXCEPT (ingested_at, rank) FROM (
SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_permit')}}) WHERE rank=1
    )

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT DISTINCT * FROM permit t1 WHERE updated_at = (SELECT MAX(updated_at) FROM permit t2 WHERE t1.id =t2.id) ORDER BY updated_at DESC
