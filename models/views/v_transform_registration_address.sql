-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH registration_address AS (
    SELECT * EXCEPT (ingested_at, rank) FROM (
SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_registration_address')}}) WHERE rank=1
    )

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT * FROM registration_address t1 WHERE created_at = (SELECT MAX(created_at) FROM registration_address t2 WHERE t1.id =t2.id) ORDER BY created_at DESC
