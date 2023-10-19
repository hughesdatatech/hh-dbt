{% macro build_jira_task_key(column='null', alias='') -%}

    '{{ var("jira_task_key_default") if column == 'null' else column }}' as {{ alias }}_jira_task_key

{%- endmacro %}
