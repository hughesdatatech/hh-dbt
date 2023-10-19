{%- macro build_predefined_multiplier_join(hcm_field_name, main_alias, build_signups_join=true, build_validity_join=true, hcm_alias_override='hcm') -%}
        left join {{ ref("ref_forecast_demo_predefined_multiplier_scd") }} as {{ hcm_alias_override }}
            on {{ hcm_alias_override }}.field_name = '{{ hcm_field_name }}'
            {{ 'and ' + main_alias + '.activity_date_at between ' + hcm_alias_override + '.signups_starting and ' + hcm_alias_override + '.signups_ending' if build_signups_join }}
            {{ 'and ' + get_run_started_date_at() + ' between ' + hcm_alias_override + '.valid_from and ' + hcm_alias_override + '.valid_through' if build_validity_join }}
{%- endmacro -%}
