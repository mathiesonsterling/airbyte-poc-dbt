SELECT *,
       DATETIME(created_at, "America/New_York") as created_at_est,
       DATETIME(appointment_time, "America/New_York") as appointment_time_est
FROM {{ ref('v_transform_permit')}}