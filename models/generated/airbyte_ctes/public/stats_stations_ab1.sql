{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_stats_stations') }}
select
    {{ json_extract_scalar('_airbyte_data', ['lat'], ['lat']) }} as lat,
    {{ json_extract_scalar('_airbyte_data', ['lon'], ['lon']) }} as lon,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['pkid'], ['pkid']) }} as pkid,
    {{ json_extract_scalar('_airbyte_data', ['rbsn'], ['rbsn']) }} as rbsn,
    {{ json_extract_scalar('_airbyte_data', ['airport'], ['airport']) }} as airport,
    {{ json_extract_scalar('_airbyte_data', ['fir_key'], ['fir_key']) }} as fir_key,
    {{ json_extract_scalar('_airbyte_data', ['is_agro'], ['is_agro']) }} as is_agro,
    {{ json_extract_scalar('_airbyte_data', ['is_rain'], ['is_rain']) }} as is_rain,
    {{ json_extract_scalar('_airbyte_data', ['is_rbcn'], ['is_rbcn']) }} as is_rbcn,
    {{ json_extract_scalar('_airbyte_data', ['is_synop'], ['is_synop']) }} as is_synop,
    {{ json_extract_scalar('_airbyte_data', ['air_force'], ['air_force']) }} as air_force,
    {{ json_extract_scalar('_airbyte_data', ['iata_code'], ['iata_code']) }} as iata_code,
    {{ json_extract_scalar('_airbyte_data', ['icao_code'], ['icao_code']) }} as icao_code,
    {{ json_extract_scalar('_airbyte_data', ['is_center'], ['is_center']) }} as is_center,
    {{ json_extract_scalar('_airbyte_data', ['is_marine'], ['is_marine']) }} as is_marine,
    {{ json_extract_scalar('_airbyte_data', ['local_name'], ['local_name']) }} as local_name,
    {{ json_extract_scalar('_airbyte_data', ['regionpkid'], ['regionpkid']) }} as regionpkid,
    {{ json_extract_scalar('_airbyte_data', ['station_id'], ['station_id']) }} as station_id,
    {{ json_extract_scalar('_airbyte_data', ['country_key'], ['country_key']) }} as country_key,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_stats_stations') }} as table_alias
-- stats_stations
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

