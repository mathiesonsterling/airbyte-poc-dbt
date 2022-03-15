SELECT
       CAST(id as INT) as id,
       CAST(created_at as TIMESTAMP) as created_at,
       * except (created_at, id)
FROM {{ source('ds_airbyte', 't_ingest_internal_note_type') }}