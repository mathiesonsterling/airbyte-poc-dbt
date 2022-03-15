SELECT 
    CAST(id as INT) as id,
    CAST(user_id as INT) as user_id,
    CAST(registration_id as INT) as registration_id,
    CAST(permit_id as INT) as permit_id,
    CAST(sales_tax_id as INT) as sales_tax_id,
    CAST(non_driver_id_id as INT) as non_driver_id_id,
    CAST(license_amendment_id as INT) as license_amendment_id,
    CAST(license_reciprocity_id as INT) as license_reciprocity_id,
    CAST(id_upgrade_id as INT) as id_upgrade_id,
    action_details,
    office,
    ingested_at,
    CAST(created_at as TIMESTAMP) as created_at
FROM {{ source('ds_airbyte', 't_ingest_action')}}