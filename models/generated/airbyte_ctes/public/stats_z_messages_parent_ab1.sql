{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_stats_z_messages_parent') }}
select
    {{ json_extract_scalar('_airbyte_data', ['length'], ['length']) }} as {{ adapter.quote('length') }},
    {{ json_extract_scalar('_airbyte_data', ['rx_csn'], ['rx_csn']) }} as rx_csn,
    {{ json_extract_scalar('_airbyte_data', ['corrected'], ['corrected']) }} as corrected,
    {{ json_extract_scalar('_airbyte_data', ['data_time'], ['data_time']) }} as data_time,
    {{ json_extract_scalar('_airbyte_data', ['is_binary'], ['is_binary']) }} as is_binary,
    {{ json_extract_scalar('_airbyte_data', ['message_id'], ['message_id']) }} as message_id,
    {{ json_extract_scalar('_airbyte_data', ['rx_channel'], ['rx_channel']) }} as rx_channel,
    {{ json_extract_scalar('_airbyte_data', ['rx_circuit'], ['rx_circuit']) }} as rx_circuit,
    {{ json_extract_scalar('_airbyte_data', ['binary_data'], ['binary_data']) }} as binary_data,
    {{ json_extract_scalar('_airbyte_data', ['text_content'], ['text_content']) }} as text_content,
    {{ json_extract_scalar('_airbyte_data', ['binary_content'], ['binary_content']) }} as binary_content,
    {{ json_extract_scalar('_airbyte_data', ['storage_duration'], ['storage_duration']) }} as storage_duration,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_stats_z_messages_parent') }} as table_alias
-- stats_z_messages_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

