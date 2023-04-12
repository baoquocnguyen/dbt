{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('stats_z_reports_parent_ab1') }}
select
    cast(tt as {{ dbt_utils.type_string() }}) as tt,
    cast(msg_id as {{ dbt_utils.type_float() }}) as msg_id,
    cast(rx_time as {{ dbt_utils.type_string() }}) as rx_time,
    cast(data_time as {{ dbt_utils.type_string() }}) as data_time,
    cast(report_id as {{ dbt_utils.type_string() }}) as report_id,
    cast(station_id as {{ dbt_utils.type_string() }}) as station_id,
    cast(aax_version as {{ dbt_utils.type_string() }}) as aax_version,
    cast(ccx_version as {{ dbt_utils.type_string() }}) as ccx_version,
    cast(station_lat as {{ dbt_utils.type_float() }}) as station_lat,
    cast(station_lon as {{ dbt_utils.type_float() }}) as station_lon,
    cast(station_pkid as {{ dbt_utils.type_float() }}) as station_pkid,
    cast(validity_end as {{ dbt_utils.type_string() }}) as validity_end,
    cast(validity_start as {{ dbt_utils.type_string() }}) as validity_start,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('stats_z_reports_parent_ab1') }}
-- stats_z_reports_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

