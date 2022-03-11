SELECT
    CAST(id as INT) as id,
    CAST(created_at as TIMESTAMP) as created_at,
    * except (created_at, id),
    _airbyte_emitted_at as ingested_at
FROM {{ source('ds_airbyte', 't_ingest_non_driver_id') }}