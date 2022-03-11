select transaction_id, last_name, first_name, status, updated_at, phone from {{ ref('v_transform_id_upgrade') }} where contact_preference_type = "PHONE"
