{% macro build_job_user_id(job_user_id=var("job_user_id_default"), alias='') -%}

    '{{ job_user_id }}' as {{ alias }}_job_user_id

{%- endmacro %}
