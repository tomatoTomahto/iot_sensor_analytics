select * from tag_mappings order by tag_id asc;

select raw.record_time,
    tags.well_id, 
    tags.sensor_name, 
    raw.value
from raw_measurements raw, tag_mappings tags
where raw.tag_id = tags.tag_id
order by raw.record_time;

select * from measurements 
where well_id=3 
order by record_time desc;

select round((unix_timestamp(record_time)+30)/60)*60 as record_timestamp,
    avg(temperature_sensor),
    avg(flow_rate),
    avg(casing_pressure)
from measurements 
where well_id = 3
group by record_timestamp
order by record_timestamp desc;
