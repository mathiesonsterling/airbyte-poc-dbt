SELECT
    CAST(id as INT) as id,
    * except (id)
FROM {{ source('ds_airbyte', 't_ingest_unsuccessful_visit_reason') }}