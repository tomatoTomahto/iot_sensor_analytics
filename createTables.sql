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
  'kudu.master_addresses' = '10.0.0.48:7051',
);

select * from well_info;

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
