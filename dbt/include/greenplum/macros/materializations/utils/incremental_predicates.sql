{% macro render_dynamic_incremental_predicate(predicate, target) %}
    {% set cleared_params = predicate | replace('generate_dynamic_incremental_predicate.', '') %}
    {% set field_with_offsets = cleared_params.split(',') %}

    {% if field_with_offsets[2] is not defined %}
        {% set right_border = none %}
    {% else %}
        {% set right_border = field_with_offsets[2] %}
    {% endif %}

    {% set sql %}
        and {{
                generate_dynamic_incremental_predicate(
                    target,
                    field_with_offsets[0],
                    field_with_offsets[1],
                    right_border
                )
            }}
    {% endset %}

    {{ return(sql) }}

{% endmacro %}
