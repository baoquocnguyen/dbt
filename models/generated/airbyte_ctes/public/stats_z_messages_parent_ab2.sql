{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('stats_z_messages_parent_ab1') }}
select
    cast({{ adapter.quote('length') }} as {{ dbt_utils.type_float() }}) as {{ adapter.quote('length') }},
    cast(rx_csn as {{ dbt_utils.type_float() }}) as rx_csn,
    {{ cast_to_boolean('corrected') }} as corrected,
    cast(data_time as {{ dbt_utils.type_string() }}) as data_time,
    {{ cast_to_boolean('is_binary') }} as is_binary,
    cast(message_id as {{ dbt_utils.type_float() }}) as message_id,
    cast(rx_channel as {{ dbt_utils.type_float() }}) as rx_channel,
    cast(rx_circuit as {{ dbt_utils.type_float() }}) as rx_circuit,
    cast(binary_data as {{ dbt_utils.type_string() }}) as binary_data,
    cast(text_content as {{ dbt_utils.type_string() }}) as text_content,
    cast(binary_content as {{ dbt_utils.type_string() }}) as binary_content,
    cast(storage_duration as {{ dbt_utils.type_string() }}) as storage_duration,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('stats_z_messages_parent_ab1') }}
-- stats_z_messages_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

