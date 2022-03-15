SELECT
    CAST(id as INT) as id,
    CAST(permit_id as INT) as permit_id,
    CAST(occurred_at as TIMESTAMP) as occurred_at,
    * except (id, permit_id, occurred_at)
FROM {{ source('ds_airbyte', 't_ingest_permit_event_log') }}