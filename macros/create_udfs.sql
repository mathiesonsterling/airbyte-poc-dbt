{% macro create_udfs() %}

create schema if not exists {{target.schema}};

{{ f_determine_transaction_end_time()}};

{{ f_calculate_work_time() }}

{% endmacro %}