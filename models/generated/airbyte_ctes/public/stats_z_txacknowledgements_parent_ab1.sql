{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_stats_z_txacknowledgements_parent') }}
select
    {{ json_extract_scalar('_airbyte_data', ['data_time'], ['data_time']) }} as data_time,
    {{ json_extract_scalar('_airbyte_data', ['message_id'], ['message_id']) }} as message_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_stats_z_txacknowledgements_parent') }} as table_alias
-- stats_z_txacknowledgements_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

