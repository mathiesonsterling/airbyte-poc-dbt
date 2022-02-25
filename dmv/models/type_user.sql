SELECT 
    CAST(id as INT) as id,
    _airbyte_emitted_at as ingested_at,
    * except (id)
FROM `playground-m-sterling`.airbyte.user