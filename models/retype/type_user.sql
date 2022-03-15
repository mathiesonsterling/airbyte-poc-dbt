SELECT 
    CAST(id as INT) as id,
    ingested_at,
    * except (id)
FROM {{ source('ds_airbyte', 't_ingest_user') }}