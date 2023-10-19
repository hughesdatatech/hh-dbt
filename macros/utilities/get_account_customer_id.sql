{#
    Temporary method to set duplicate customer_ids to null for accounts that are not truly customers.
    The duplicate customer_ids cause problems in downstream dimensions.
#}
{%- macro get_account_customer_id() -%}
    case 
        when account_id || '|' || sf_client_id::integer::string in ({{ var("account_client_exclusion_list") }}) 
                or sf_client_id::integer = 0
            then null
        else sf_client_id
    end::integer as customer_id
{%- endmacro -%}
