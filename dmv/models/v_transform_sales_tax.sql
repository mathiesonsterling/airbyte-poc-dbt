-- GET THE FRESH DATA FROM THE INGEST TABLE USING ingested_at
WITH sales_tax AS (
SELECT * EXCEPT (ingested_at, rank) FROM (
    SELECT *, RANK() OVER(PARTITION BY id ORDER BY ingested_at DESC) rank FROM {{ ref('type_sales_tax')}}) WHERE rank=1
)

-- THEN GET THE NEWEST TABLE DATA USING created_at
SELECT * FROM sales_tax t1 WHERE updated_at = (SELECT MAX(updated_at) FROM sales_tax t2 WHERE t1.id =t2.id) ORDER BY updated_at DESC
