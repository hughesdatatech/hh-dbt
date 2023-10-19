{% macro build_job_id(job_id=var("job_id_default"), alias='') -%}

    '{{ job_id }}' as {{ alias }}_job_id

{%- endmacro %}
