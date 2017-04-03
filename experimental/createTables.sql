/* Create Historian Database */
create database historian;

/* Create external Impala tables that map to Kudu tables */
/* Measurements - enriched and pivoted sensor data with well & sensor information */
CREATE EXTERNAL TABLE `measurements` STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'measurements',
  'kudu.master_addresses' = 'ip-10-0-0-67.us-west-2.compute.internal:7051');

/* Raw measurements - raw sensor data from well with no only tag id of sensor */
CREATE EXTERNAL TABLE `raw_measurements` STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'raw_measurements',
  'kudu.master_addresses' = 'ip-10-0-0-67.us-west-2.compute.internal:7051');

/* Tag Mappings - mappings of tag ids to well & sensor information for enrichment */
CREATE EXTERNAL TABLE `tag_mappings` STORED AS KUDU
TBLPROPERTIES(
  'kudu.table_name' = 'tag_mappings',
  'kudu.master_addresses' = 'ip-10-0-0-67.us-west-2.compute.internal:7051');

