{% test row_count(model, column_name, above) %}
select {{ above }} - row_count
from (select count({{ column_name }}) as row_count from {{ model }})
where row_count <= {{ above }}
{% endtest %}