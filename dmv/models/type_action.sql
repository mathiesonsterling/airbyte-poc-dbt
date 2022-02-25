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
    _airbyte_emitted_at as ingested_at_badly,
    action_details,
    office,
    created_at
FROM `playground-m-sterling`.ds_transform.action