{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_stats_z_reports_parent') }}
select
    {{ json_extract_scalar('_airbyte_data', ['tt'], ['tt']) }} as tt,
    {{ json_extract_scalar('_airbyte_data', ['msg_id'], ['msg_id']) }} as msg_id,
    {{ json_extract_scalar('_airbyte_data', ['content'], ['content']) }} as {{ adapter.quote('content') }},
    {{ json_extract_scalar('_airbyte_data', ['rx_time'], ['rx_time']) }} as rx_time,
    {{ json_extract_scalar('_airbyte_data', ['data_time'], ['data_time']) }} as data_time,
    {{ json_extract_scalar('_airbyte_data', ['report_id'], ['report_id']) }} as report_id,
    {{ json_extract_scalar('_airbyte_data', ['station_id'], ['station_id']) }} as station_id,
    {{ json_extract_scalar('_airbyte_data', ['aax_version'], ['aax_version']) }} as aax_version,
    {{ json_extract_scalar('_airbyte_data', ['ccx_version'], ['ccx_version']) }} as ccx_version,
    {{ json_extract_scalar('_airbyte_data', ['station_lat'], ['station_lat']) }} as station_lat,
    {{ json_extract_scalar('_airbyte_data', ['station_lon'], ['station_lon']) }} as station_lon,
    {{ json_extract_scalar('_airbyte_data', ['station_pkid'], ['station_pkid']) }} as station_pkid,
    {{ json_extract_scalar('_airbyte_data', ['validity_end'], ['validity_end']) }} as validity_end,
    {{ json_extract_scalar('_airbyte_data', ['validity_start'], ['validity_start']) }} as validity_start,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_stats_z_reports_parent') }} as table_alias
-- stats_z_reports_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

