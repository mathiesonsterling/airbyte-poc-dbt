SELECT
    CAST(id as INT) as id,
    * except (id),
     _airbyte_emitted_at as ingested_at
FROM {{ source('ds_airbyte', 't_ingest_document') }}