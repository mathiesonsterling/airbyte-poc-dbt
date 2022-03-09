SELECT *,
       DATETIME(created_at, "America/New_York") as created_at_est,
       DATETIME(updated_at, "America/New_York") as updated_at_est
FROM {{ ref('v_transform_registration')}}
