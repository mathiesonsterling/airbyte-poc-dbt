SELECT
    CAST(id as INT) as id,
    CAST(created_at as TIMESTAMP) as created_at,
    CAST(updated_at as TIMESTAMP) as updated_at,
    * except (id, created_at, updated_at)
FROM {{ source('ds_airbyte', 't_ingest_registration') }}