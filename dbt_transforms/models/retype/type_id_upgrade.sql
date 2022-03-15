SELECT
    CAST(id as INT) as id,
    CAST(created_at AS TIMESTAMP) as created_at,
    * except (id, created_at)
FROM {{ source('ds_airbyte', 't_ingest_id_upgrade') }}