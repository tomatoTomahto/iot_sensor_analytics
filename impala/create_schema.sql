create database sensors;
use sensors;

drop table if exists wells;
drop table if exists asset_groups;
drop table if exists well_assets;
drop table if exists asset_sensors;
drop table if exists measurements;
drop table if exists maintenance;

create table wells (
    well_id int,
    latitude float,
    longitude float,
    depth int,
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
    value double,
    primary key (time, sensor_id)
) 
partition by hash partitions 16
stored as kudu;

create table maintenance (
    maint_date bigint,
    asset_id int,
    cost double,
    duration int,
    type string,
    primary key (maint_date, asset_id)
) 
partition by hash partitions 16
stored as kudu;
