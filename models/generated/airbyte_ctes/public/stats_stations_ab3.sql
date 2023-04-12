{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('stats_stations_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'lat',
        'lon',
        adapter.quote('name'),
        'pkid',
        boolean_to_string('rbsn'),
        boolean_to_string('airport'),
        'fir_key',
        boolean_to_string('is_agro'),
        boolean_to_string('is_rain'),
        boolean_to_string('is_rbcn'),
        boolean_to_string('is_synop'),
        boolean_to_string('air_force'),
        'iata_code',
        'icao_code',
        boolean_to_string('is_center'),
        boolean_to_string('is_marine'),
        'local_name',
        'regionpkid',
        'station_id',
        'country_key',
    ]) }} as _airbyte_stats_stations_hashid,
    tmp.*
from {{ ref('stats_stations_ab2') }} tmp
-- stats_stations
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

