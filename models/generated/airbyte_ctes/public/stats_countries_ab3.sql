{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('stats_countries_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'code',
        'iso2',
        adapter.quote('name'),
        'pkid',
        'icao_id',
        'country_id',
        'local_name',
    ]) }} as _airbyte_stats_countries_hashid,
    tmp.*
from {{ ref('stats_countries_ab2') }} tmp
-- stats_countries
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

