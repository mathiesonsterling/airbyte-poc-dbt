{{ config(materialized='table') }}
SELECT * FROM {{ ref('v_transform_action') }}