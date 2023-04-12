{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('stats_z_messages_parent_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('length'),
        'rx_csn',
        boolean_to_string('corrected'),
        'data_time',
        boolean_to_string('is_binary'),
        'message_id',
        'rx_channel',
        'rx_circuit',
        'binary_data',
        'text_content',
        'binary_content',
        'storage_duration',
    ]) }} as _airbyte_stats_z_messages_parent_hashid,
    tmp.*
from {{ ref('stats_z_messages_parent_ab2') }} tmp
-- stats_z_messages_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

