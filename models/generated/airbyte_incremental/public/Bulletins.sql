{{ config(
    indexes = [{'columns':['message_id'],'type':'btree'}],
    unique_key = 'message_id',
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('stats_z_messages_parent_ab3') }}
select
    gts.cccc,
    messages.text_content,
    messages.storage_duration::timestamp without time zone Observation_date,
    messages.storage_duration::timestamp without time zone::time Observation_time,
    messages.data_time::timestamp without time zone Reception_date,
    messages.data_time::timestamp without time zone::time Reception_time,
    tx_ack.data_time::timestamp without time zone Sending_date,
    tx_ack.data_time::timestamp without time zone::time Sending_time,
    DATE_PART('day', messages.data_time::timestamp - messages.storage_duration::timestamp) * 24 * 60 +
    DATE_PART('hour', messages.data_time::timestamp - messages.storage_duration::timestamp) * 60 +
    DATE_PART('minute', messages.data_time::timestamp - messages.storage_duration::timestamp) Minutes_between_observation_and_reception,
    DATE_PART('day', tx_ack.data_time::timestamp - messages.storage_duration::timestamp) * 24 * 60 +
    DATE_PART('hour', tx_ack.data_time::timestamp - messages.storage_duration::timestamp) * 60 +
    DATE_PART('minute', tx_ack.data_time::timestamp - messages.storage_duration::timestamp) Minutes_between_observation_and_sending,
    case
     when gts.bbb like 'R%' then 'Retard'
     when gts.bbb like 'C%' then 'Corrected'
     when gts.bbb like 'A%' then 'Amended'
     else 'Initial'
    end Message_type,
    gts.ttaaii,
    CASE when tx_ack.message_id IS NOT NULL then 'sent' else 'received' end Origin,
    messages.message_id
from {{ ref('stats_z_messages_parent_ab3') }} messages
left join {{ ref('stats_z_gts_parent_ab3') }} gts on gts.message_id = messages.message_id
left JOIN {{ ref('stats_z_txacknowledgements_parent_ab3') }} tx_ack
on tx_ack.message_id = messages.message_id
-- stats_z_messages_parent from {{ source('public', '_airbyte_raw_stats_z_messages_parent') }}
where 1 = 1

