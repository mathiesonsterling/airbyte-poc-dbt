SELECT
    CAST(id as INT) as id,
    CAST(registration_id as INT) as registration_id,
    _airbyte_emitted_at as ingested_at,
    * except(id, registration_id)
FROM {{ source('ds_airbyte', 't_ingest_boat_information') }}