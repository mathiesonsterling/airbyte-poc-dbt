SELECT
    CAST(id as INT) as id,
    CAST(permit_id as INT) as permit_id,
    CAST(created_at as TIMESTAMP) as created_at,
    * except (id, permit_id, created_at),
    _airbyte_emitted_at as ingested_at
FROM {{ source('ds_airbyte', 't_ingest_permit_test_block_event_log') }}