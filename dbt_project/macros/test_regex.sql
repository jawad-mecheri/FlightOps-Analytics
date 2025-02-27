{% test regex(model, column_name, regex_pattern) %}
  with validation as (
    select
      {{ column_name }} as value_field
    from {{ model }}
  )
  select *
  from validation
  where value_field is not null
    and not regexp_contains(cast(value_field as string), {{ regex_pattern }})
{% endtest %}
