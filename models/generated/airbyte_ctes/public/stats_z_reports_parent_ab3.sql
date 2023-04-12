{{ config(
    indexes = [{'columns':['report_id'],'type':'btree'}],
    unique_key = 'report_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('stats_z_reports_parent_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'tt',
        'msg_id',
        'rx_time',
        'data_time',
        'report_id',
        'station_id',
        'aax_version',
        'ccx_version',
        'station_lat',
        'station_lon',
        'station_pkid',
        'validity_end',
        'validity_start'
    ]) }} as _airbyte_stats_z_reports_parent_hashid,
    tmp.*
from {{ ref('stats_z_reports_parent_ab2') }} tmp
-- stats_z_reports_parent
where 1 = 1
{{ incremental_clause('report_id') }}

