select * from well_info;
select * from well_tags;
select * from well_measurements_raw;
select * from well_measurements order by record_time desc, well_id asc;
select well_id, count(distinct(record_time)) from well_measurements group by well_id order by well_id;
delete from sensor_measurements;

DROP TABLE IF EXISTS well_info;
CREATE TABLE well_info (
    well_id INTEGER,
    latitude FLOAT,
    longitude FLOAT,
    depth INTEGER,
    well_type STRING,
    well_chemical STRING,
    leakage_risk FLOAT
)
DISTRIBUTE BY HASH (well_id) INTO 9 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'well_info',
  'kudu.master_addresses' = '10.0.0.106:7051',
  'kudu.key_columns' = 'well_id'
);

DROP TABLE IF EXISTS well_tags;
CREATE TABLE well_tags (
    tag_id INTEGER,
    well_id INTEGER,
    tag_entity STRING
)
DISTRIBUTE BY HASH (tag_id) INTO 9 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'well_tags',
  'kudu.master_addresses' = '10.0.0.106:7051',
  'kudu.key_columns' = 'tag_id'
);

DROP TABLE IF EXISTS well_measurements_raw;
CREATE TABLE well_measurements_raw (
    record_time BIGINT,
    tag_id INTEGER,
    value DOUBLE
)
DISTRIBUTE BY HASH (tag_id) INTO 9 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'well_measurements_raw',
  'kudu.master_addresses' = '10.0.0.106:7051',
  'kudu.key_columns' = 'record_time, tag_id'
);

DROP TABLE IF EXISTS well_measurements;
CREATE TABLE well_measurements (
    record_time BIGINT,
    well_id INTEGER,
    casing_pressure DOUBLE,
    choke_value_opening DOUBLE,
    flow_rate DOUBLE,
    temperature_sensor DOUBLE,
    tubing_pressure DOUBLE
)
DISTRIBUTE BY HASH (well_id) INTO 9 BUCKETS
TBLPROPERTIES(
  'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
  'kudu.table_name' = 'well_measurements',
  'kudu.master_addresses' = '10.0.0.106:7051',
  'kudu.key_columns' = 'record_time, well_id'
);

