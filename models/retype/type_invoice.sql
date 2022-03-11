SELECT 
    CAST(id	AS INT) AS id,
    amount,
    office,	
    status,	
    CAST(user_id AS INT) AS	user_id,	
    initials,	
    CAST(permit_id AS INT) AS permit_id,	
    CAST(created_at AS TIMESTAMP) as created_at,
    _airbyte_emitted_at as ingested_at,
    CAST(updated_at as TIMESTAMP) as updated_at,
    payment_type,	
    CAST(sales_tax_id AS INT) AS sales_tax_id,	
    CAST(id_upgrade_id AS INT) AS id_upgrade_id,	
    invoice_number,	
    CAST(previous_amount AS INT) AS	previous_amount,	
    CAST(registration_id AS INT) AS	registration_id,	
    CAST(non_driver_id_id AS INT) AS non_driver_id_id,
    CAST(license_amendment_id AS INT) AS license_amendment_id,	
    CAST(license_reciprocity_id	AS INT) AS license_reciprocity_id	
FROM {{ source('ds_airbyte', 't_ingest_invoice') }}