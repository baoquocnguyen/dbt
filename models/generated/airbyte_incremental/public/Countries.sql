{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('stats_countries_ab3') }}
select
    code,
    iso2,
    {{ adapter.quote('name') }},
    pkid,
    icao_id,
    country_id,
    local_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_stats_countries_hashid
from {{ ref('stats_countries_ab3') }}
-- stats_countries from {{ source('public', '_airbyte_raw_stats_countries') }}
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

