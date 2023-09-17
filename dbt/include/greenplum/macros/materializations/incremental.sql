{% macro get_incremental_truncate_insert_sql(arg_dict) %}

  {% do return(greenplum__get_truncate_insert_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["dest_columns"])) %}

{% endmacro %}

{% macro greenplum__get_truncate_insert_sql(target, source, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    truncate {{ target }};

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}


{% macro greenplum__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target }}
            using {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        {% if predicate.startswith('generate_dynamic_incremental_predicate.') %}
                            {{ render_dynamic_incremental_predicate(predicate, target) }}
                        {% else %}
                            and {{ predicate }}
                        {% endif %}
                    {% endfor %}
                {% endif %}
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    {% if predicate.startswith('generate_dynamic_incremental_predicate.') %}
                        {{ render_dynamic_incremental_predicate(predicate, target) }}
                    {% else %}
                        and {{ predicate }}
                    {% endif %}
                {% endfor %}
            {%- endif -%};

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}
