SELECT
  * except (id, user_id, created_at, updated_at, contractor_id),
CAST(id AS INT) as id,
CAST(user_id as INT) as user_id,
CAST(created_at as TIMESTAMP) as created_at,
CAST(updated_at as TIMESTAMP) as updated_at,
CAST(contractor_id as INT) as contractor_id,
FROM {{ source('ds_airbyte', 't_ingest_sales_tax') }}