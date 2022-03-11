-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH non_driver_id AS (
    SELECT * EXCEPT (ingested_at, rank) FROM (
SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_non_driver_id')}}) WHERE rank=1
    )

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT * FROM non_driver_id t1 WHERE updated_at = (SELECT MAX(updated_at) FROM non_driver_id t2 WHERE t1.id =t2.id) ORDER BY updated_at DESC
