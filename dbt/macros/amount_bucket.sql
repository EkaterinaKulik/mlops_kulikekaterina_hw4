{% macro amount_bucket(amount_column) %}
    case
        when {{ amount_column }} < 50 then 'LOW'
        when {{ amount_column }} < 200 then 'MEDIUM'
        when {{ amount_column }} < 1000 then 'HIGH'
        else 'VERY_HIGH'
    end
{% endmacro %}
