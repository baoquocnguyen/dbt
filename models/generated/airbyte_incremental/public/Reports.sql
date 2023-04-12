{{ config(
    indexes = [{'columns':['report_id'],'type':'btree'}],
    unique_key = 'report_id',
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('stats_z_reports_parent_ab3') }}
select
    reports.rx_time::timestamp without time zone Reception_Date,
    tx_ack.data_time::timestamp without time zone Sending_date,
    reports.data_time::timestamp without time zone Observation_date,
    reports.validity_end::timestamp without time zone Start_of_Validity,
    reports.validity_start::timestamp without time zone End_of_validity,
    CASE when tx_ack.message_id IS NOT NULL then 'sent' else 'received' end Origin,
    DATE_PART('day', reports.rx_time::timestamp - reports.data_time::timestamp) * 24 * 60 +
    DATE_PART('hour', reports.rx_time::timestamp - reports.data_time::timestamp) * 60 +
    DATE_PART('minute', reports.rx_time::timestamp - reports.data_time::timestamp) Minutes_between_observation_and_reception,
    DATE_PART('day', tx_ack.data_time::timestamp - reports.data_time::timestamp) * 24 * 60 +
    DATE_PART('hour', tx_ack.data_time::timestamp - reports.data_time::timestamp) * 60 +
    DATE_PART('minute', tx_ack.data_time::timestamp - reports.data_time::timestamp) Minutes_between_observation_and_sending,
    reports.station_id Station_code,
    reports.station_lat Latitude,
    reports.station_lon Longitude,
    case when reports.tt in ('SM', 'SMV', 'SN', 'SI', 'SS', 'ISM', 'ISI', 'ISN', 'ISA') then 'SYNOP'
         when reports.tt in ('SA', 'LA' ) then 'METAR'
         when reports.tt in ('SP', 'LP') then 'SPECI'
         when reports.tt in ('FT', 'FC', 'LT', 'LC') then 'TAF'
         when reports.tt in ('WS', 'WV', 'WC', 'LS', 'LV', 'LY') then 'SIGMET'
         when reports.tt in ('WT') then 'TROPICAL CYCLONE'
         when reports.tt in ('UA') then 'AIREP'
         when reports.tt in ('UD', 'IUO', 'IUA') then 'AMDAR'
         when reports.tt in ('US', 'UK', 'IUS', 'IUK') then 'TEMP'
         when reports.tt in ('UP', 'UG', 'IUJ', 'IUP', 'IUW') then 'PILOT'
         when reports.tt in ('CS', 'ISC') then 'CLIMAT'
         when reports.tt in ('IS') then 'SURFACE'
         when reports.tt in ('ISS') then 'SHIP'
         when reports.tt in ('IOB') then 'BUOY'
         when reports.tt in ('IUC') then 'SATOB'
        end Report_type,
    countries.name Country,
    stations.name Station,
    reports.tt TT,
    case when reports.aax_version is not null then ASCII(reports.aax_version) - 64 else 0 end Number_of_amendments,
    case when reports.ccx_version is not null then ASCII(reports.ccx_version) - 64 else 0 end Number_of_corrections,
    reports.report_id
from {{ ref('stats_z_reports_parent_ab3') }} reports
-- stats_z_reports_parent from {{ source('public', '_airbyte_raw_stats_z_reports_parent') }}
left JOIN {{ ref('stats_z_txacknowledgements_parent_ab3') }} tx_ack
on tx_ack.message_id = reports.msg_id
left JOIN {{ ref('stats_stations_ab3') }} stations
on stations.pkid = reports.station_pkid
left JOIN {{ ref('stats_countries_ab3') }} countries
on countries.pkid = stations.country_key
where 1 = 1

