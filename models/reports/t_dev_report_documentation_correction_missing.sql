select
    id as action_id,

    CASE
        WHEN action_details like "%set a status of NeedsCorrections%" Then
            substr(ARRAY_REVERSE(SPLIT(action_details, 'NeedsCorrections on document '))[ORDINAL(1)], 1,
                   character_length(ARRAY_REVERSE(SPLIT(action_details, 'NeedsCorrections on document '))[ORDINAL(1)]) - 22)

        WHEN action_details like "%set a status of Missing on document%" Then
            substr(ARRAY_REVERSE(SPLIT(action_details, 'set a status of Missing on document '))[ORDINAL(1)], 1,
                   character_length(ARRAY_REVERSE(SPLIT(action_details, 'set a status of Missing on document '))[ORDINAL(1)]) -22)
        ELSE "N/A"
        END as document,

    CASE
        WHEN action_details like "%set a status of NeedsCorrections%" Then "Needs Corrections"

        WHEN action_details like "%set a status of Missing on document%" Then "Missing"
        END as missing_or_correction,

    CASE
        WHEN registration_id is not null Then "Original Registration"
        WHEN permit_id is not null Then "Original Permit"
        END as transaction_type,
    CAST(created_at AS DATE) as day
from {{ ref('v_transform_action') }}
where action_details like "%set a status of NeedsCorrections%" or action_details like "%set a status of Missing on document%"
