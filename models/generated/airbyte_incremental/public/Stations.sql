{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('stats_stations_ab3') }}
select
    lat,
    lon,
    {{ adapter.quote('name') }},
    pkid,
    rbsn,
    airport,
    fir_key,
    is_agro,
    is_rain,
    is_rbcn,
    is_synop,
    air_force,
    iata_code,
    icao_code,
    is_center,
    is_marine,
    local_name,
    regionpkid,
    station_id,
    country_key,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_stats_stations_hashid
from {{ ref('stats_stations_ab3') }}
-- stats_stations from {{ source('public', '_airbyte_raw_stats_stations') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

