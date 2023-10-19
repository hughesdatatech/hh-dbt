{{
    config(
        materialized='view'
    )
}}

{{ build_rpt_legacy_marketing_touchpoints_historical_model(is_prod_forecast_model=False) }}
