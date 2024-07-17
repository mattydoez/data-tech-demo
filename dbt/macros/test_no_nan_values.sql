{% macro test_no_nan_values(model, model_name) %}
    {# Fetch all column names for the given model #}
    {% set columns = adapter.get_columns_in_relation(ref(model_name)) %}
    {% set column_tests = [] %}

    {# Generate a query for each column to check for 'NaN' values #}
    {% for column in columns %}
        {% if column.data_type in ["varchar", "text", "char"] %}
            {% set column_test = "SELECT * FROM " ~ ref(model_name) ~ " WHERE " ~ column.name ~ " = 'NaN'" %}
            {% do column_tests.append(column_test) %}
        {% endif %}
    {% endfor %}

    {# Combine all column tests into a single union query #}
    {% if column_tests | length == 0 %}
        SELECT 1
    {% else %}
        {% set union_query = column_tests | join(' UNION ALL ') %}
        {{ union_query }}
    {% endif %}
{% endmacro %}