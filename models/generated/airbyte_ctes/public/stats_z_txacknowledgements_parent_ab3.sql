{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('stats_z_txacknowledgements_parent_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'data_time',
        'message_id',
    ]) }} as _airbyte_stats_z_txa__gements_parent_hashid,
    tmp.*
from {{ ref('stats_z_txacknowledgements_parent_ab2') }} tmp
-- stats_z_txacknowledgements_parent
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

