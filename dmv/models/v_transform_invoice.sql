WITH invoice AS (
SELECT * EXCEPT (ingested_at, rank) FROM (
    SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_invoice') }}) WHERE rank=1
)

-- THEN GET THE NEWEST TABLE DATA USING updated_at
SELECT DISTINCT * FROM invoice t1 WHERE updated_at = (SELECT MAX(updated_at) FROM invoice t2 WHERE t1.id =t2.id) ORDER BY updated_at DESC
