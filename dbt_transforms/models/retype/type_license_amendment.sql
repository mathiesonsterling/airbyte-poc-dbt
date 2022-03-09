SELECT
    CAST(id as INT) as id,
    CAST(created_at as TIMESTAMP) as created_at,
    CAST(updated_at as TIMESTAMP) as updated_at,
    * except (created_at, id, updated_at),
    _airbyte_emitted_at as ingested_at
FROM {{ source('ds_airbyte', 't_ingest_license_amendment') }}