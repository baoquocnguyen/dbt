{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('stats_stations_ab1') }}
select
    cast(lat as {{ dbt_utils.type_float() }}) as lat,
    cast(lon as {{ dbt_utils.type_float() }}) as lon,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('name') }},
    cast(pkid as {{ dbt_utils.type_float() }}) as pkid,
    {{ cast_to_boolean('rbsn') }} as rbsn,
    {{ cast_to_boolean('airport') }} as airport,
    cast(fir_key as {{ dbt_utils.type_float() }}) as fir_key,
    {{ cast_to_boolean('is_agro') }} as is_agro,
    {{ cast_to_boolean('is_rain') }} as is_rain,
    {{ cast_to_boolean('is_rbcn') }} as is_rbcn,
    {{ cast_to_boolean('is_synop') }} as is_synop,
    {{ cast_to_boolean('air_force') }} as air_force,
    cast(iata_code as {{ dbt_utils.type_string() }}) as iata_code,
    cast(icao_code as {{ dbt_utils.type_string() }}) as icao_code,
    {{ cast_to_boolean('is_center') }} as is_center,
    {{ cast_to_boolean('is_marine') }} as is_marine,
    cast(local_name as {{ dbt_utils.type_string() }}) as local_name,
    cast(regionpkid as {{ dbt_utils.type_float() }}) as regionpkid,
    cast(station_id as {{ dbt_utils.type_string() }}) as station_id,
    cast(country_key as {{ dbt_utils.type_float() }}) as country_key,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('stats_stations_ab1') }}
-- stats_stations
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

