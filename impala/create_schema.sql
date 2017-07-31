/* Create a database called "sensors" */
create database sensors;
use sensors;

/* Drop existing tables if any */
drop table if exists wells;
drop table if exists asset_groups;
drop table if exists well_assets;
drop table if exists asset_sensors;
drop table if exists measurements;
drop table if exists maintenance;
drop table if exists pivot_measurements;
drop view if exists 15m_agg_pivot_measurements;

/* Create all tables */
create table wells (
    well_id int,
    latitude float,
    longitude float,
    depth int,
    chemical string,
    primary key (well_id)
) 
partition by hash partitions 16
stored as kudu;

create table asset_groups (
    group_id int,
    group_name string,
    primary key (group_id)
) partition by hash partitions 16
stored as kudu;

create table well_assets (
    asset_id int,
    well_id int,
    asset_name string,
    asset_group_id int,
    primary key (asset_id)
) partition by hash partitions 16
stored as kudu;

create table asset_sensors (
    sensor_id int,
    asset_id int,
    sensor_name string,
    units string,
    primary key (sensor_id)
) partition by hash partitions 16
stored as kudu;

create table measurements (
    time bigint,
    sensor_id int,
    value float,
    primary key (time, sensor_id)
) 
partition by hash partitions 16
stored as kudu;

create table maintenance (
    maint_date bigint,
    asset_id int,
    cost float,
    duration int,
    type string,
    primary key (maint_date, asset_id)
) 
partition by hash partitions 16
stored as kudu;

/* Test table that stream processing will write into */
create table pivot_measurements (
    well_id int,
    time bigint,
    sensor_1 float,
    sensor_2 float,
    sensor_3 float,
    primary key (well_id, time)
) partition by hash partitions 16
stored as kudu;

/* Dynamic time-series aggregation using Kudu */
create view 15m_agg_pivot_measurements as
select from_unixtime(cast(round(time/900+450)*900 as bigint)) as time,
    well_id, 
    avg(sensor_1) as sensor_1,
    avg(sensor_2) as sensor_2,
    avg(sensor_3) as sensor_3
from pivot_measurements
group by well_id, time
order by time, well_id desc;

/* Delete from all tables */
delete from wells;
delete from asset_groups;
delete from well_assets;
delete from asset_sensors;
delete from measurements;
delete from maintenance;
delete from pivot_measurements;