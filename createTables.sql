select * from well_info;
select * from tag_mappings;
select * from sensor_measurements order by well, record_time;
select well, count(distinct(record_time)) from sensor_measurements group by well order by well;
delete from sensor_measurements;

DROP TABLE IF EXISTS well_info;
CREATE TABLE well_info (
    well_id INTEGER,
    latitude FLOAT,
    longitude FLOAT,
    depth INTEGER,
    well_type STRING,
    well_chemical STRING,
    leakage_risk FLOAT,
    PRIMARY KEY (well_id)
)
DISTRIBUTE BY HASH (well_id) INTO 9 BUCKETS
STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'well_info',
  'kudu.master_addresses' = '10.0.0.48:7051'
);

DROP TABLE IF EXISTS tag_mappings;
CREATE TABLE tag_mappings (
    tag_id INTEGER,
    well STRING,
    tag_entity STRING,
    PRIMARY KEY (tag_id)
)
DISTRIBUTE BY HASH (tag_id) INTO 9 BUCKETS
STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'tag_mappings',
  'kudu.master_addresses' = '10.0.0.48:7051'
);

CREATE TABLE sensor_data (
    tag_id INTEGER,
    value INTEGER,
    PRIMARY KEY (tag_id)
)
DISTRIBUTE BY HASH (tag_id) INTO 9 BUCKETS
STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'sensor_data',
  'kudu.master_addresses' = '10.0.0.48:7051'
);

DROP TABLE IF EXISTS sensor_measurements;
CREATE TABLE sensor_measurements (
    record_time BIGINT,
    well STRING,
    casing_pressure DOUBLE NULL,
    choke_value_opening DOUBLE NULL,
    flow_rate DOUBLE NULL,
    temperature_sensor DOUBLE NULL,
    tubing_pressure DOUBLE NULL,
    PRIMARY KEY (record_time, well)
)
DISTRIBUTE BY HASH (well) INTO 9 BUCKETS
STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'sensor_measurements',
  'kudu.master_addresses' = '10.0.0.48:7051'
);
