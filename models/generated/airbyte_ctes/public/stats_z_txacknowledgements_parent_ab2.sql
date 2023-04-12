{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('stats_z_txacknowledgements_parent_ab1') }}
select
    cast(data_time as {{ dbt_utils.type_string() }}) as data_time,
    cast(message_id as {{ dbt_utils.type_float() }}) as message_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('stats_z_txacknowledgements_parent_ab1') }}
-- stats_z_txacknowledgements_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

