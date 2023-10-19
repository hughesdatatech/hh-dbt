{% macro select_debezium_metadata_cols(select_from_source=True) -%}

    {% if select_from_source %}

        op                  as _dbz_meta__op,
        __ingested_at       as _dbz_meta__ingested_at, 
        __ingestion_process as _dbz_meta__ingestion_process, 
        __source_ts_raw     as _dbz_meta__source_ts_raw, 
        __metadata          as _dbz_meta__metadata, 
        __source_ts         as _dbz_meta__source_ts, 
        __is_deleted        as _dbz_meta__is_deleted, 
        __source_lsn        as _dbz_meta__source_lsn, 
        __debezium_ts_ms    as _dbz_meta__debezium_ts_ms, 
        __sequence_by       as _dbz_meta__sequence_by

    {% else %}

        _dbz_meta__op,
        _dbz_meta__ingested_at, 
        _dbz_meta__ingestion_process, 
        _dbz_meta__source_ts_raw, 
        _dbz_meta__metadata, 
        _dbz_meta__source_ts, 
        _dbz_meta__is_deleted, 
        _dbz_meta__source_lsn, 
        _dbz_meta__debezium_ts_ms, 
        _dbz_meta__sequence_by

    {% endif %}

{%- endmacro %}
