{% test accepted_range(model, column_name, min_value, max_value=None) %}
select *
from {{ model }}
where {{ column_name }} < {{ min_value }}
{% if max_value is not none %}
   or {{ column_name }} > {{ max_value }}
{% endif %}
{% endtest %}
