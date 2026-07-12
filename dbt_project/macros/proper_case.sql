{% macro proper_case(column) %}
    array_to_string(
        list_transform(
            string_split(lower(trim({{ column }})), ' '),
            word -> array_to_string(
                list_transform(
                    string_split(word, '-'),
                    piece -> upper(substr(piece, 1, 1)) || substr(piece, 2)
                ),
                '-'
            )
        ),
        ' '
    )
{% endmacro %}
