{% test accepted_values_bool(model, column_name, values) %}
with accepted as (
    {% for v in values %}
      select '{{ v | lower }}' as value{% if not loop.last %} union all {% endif %}
    {% endfor %}
)
select *
from {{ model }}
where cast({{ column_name }} as string) not in (
    select value from accepted
)
{% endtest %}
