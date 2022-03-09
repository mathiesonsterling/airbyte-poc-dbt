-- Convert ip_address to binary, then to hex string, then check the left 12 bytes (2 chars/byte) for "IPv4 as IPv6" value
{% macro r_transform_ipv6to4(column_name ) %}
CASE SUBSTR(CAST(NET.IP_FROM_STRING({{column_name }}) AS STRING FORMAT 'HEX'), 0, 24)
    WHEN '00000000000000000000ffff' THEN
      -- Convert the last 4 bytes back to an ip_address, getting us the IPv4 format
      NET.IP_TO_STRING(CAST(SUBSTR(CAST(NET.IP_FROM_STRING({{column_name }}) AS STRING FORMAT 'HEX'), 25, 8) AS BYTES FORMAT 'HEX'))
    ELSE
      -- else we have an actual IPv6 value and cannot convert to IPv4, or it was already IPv4
      {{column_name}}
END
{% endmacro %}