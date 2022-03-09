-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH county AS (
    SELECT * EXCEPT (ingested_at, rank) FROM (
SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_county')}}) WHERE rank=1
    )

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT DISTINCT * FROM county t1 WHERE created_at = (SELECT MAX(created_at) FROM county t2 WHERE t1.id =t2.id) ORDER BY created_at DESC
