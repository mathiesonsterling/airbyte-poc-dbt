SELECT
    CAST(id as INT) as id,
    CAST(created_at as TIMESTAMP) as created_at,
    CAST(updated_at as TIMESTAMP) as updated_at,
    CAST(submitted_on as TIMESTAMP) as submitted_on,
    CAST(test_date as TIMESTAMP) as test_date,
    CAST(appointment_time as TIMESTAMP) as appointment_time,
    * except (id, submitted_on, created_at, updated_at, test_date, appointment_time)
FROM {{ source('ds_airbyte', 't_ingest_permit') }}