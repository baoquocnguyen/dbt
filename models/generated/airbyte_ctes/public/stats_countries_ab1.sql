{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_stats_countries') }}
select
    {{ json_extract_scalar('_airbyte_data', ['code'], ['code']) }} as code,
    {{ json_extract_scalar('_airbyte_data', ['iso2'], ['iso2']) }} as iso2,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as {{ adapter.quote('name') }},
    {{ json_extract_scalar('_airbyte_data', ['pkid'], ['pkid']) }} as pkid,
    {{ json_extract_scalar('_airbyte_data', ['icao_id'], ['icao_id']) }} as icao_id,
    {{ json_extract_scalar('_airbyte_data', ['country_id'], ['country_id']) }} as country_id,
    {{ json_extract_scalar('_airbyte_data', ['local_name'], ['local_name']) }} as local_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_stats_countries') }} as table_alias
-- stats_countries
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

